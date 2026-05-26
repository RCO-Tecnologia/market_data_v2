# market_data_v2 — Arquitetura

> Documento canônico de arquitetura do pipeline de ingestão e distribuição de dados de mercado da Cedro Crystal. Toda decisão de design deve ser consistente com este documento; mudanças significativas exigem atualização explícita aqui.
>
> Versão: 1.1 — 2026-05-26
> Status: Aprovada para implementação
>
> Histórico:
> - 1.0 (2026-05-26): versão inicial.
> - 1.1 (2026-05-26): revisão crítica final pré-implementação — adoção de jemalloc, ahash, SO_RCVBUF 128MB com limiares mais agressivos (warn 20%, panic 40%), regra "sem logs no hot path" (§5.8), HA assimétrica documentada (§11.4), deployment tuning checklist (§11.5). Tokio mantido vs Glommio; Redis mantido na v1 (in-process via NATS deferido). DragonflyDB documentado como upgrade futuro.

---

## 1. Visão Geral e Objetivos

O sistema lê o socket TCP da **Cedro Crystal** (porta 81, protocolo ASCII streaming descrito em [`api.md`](./api.md) e [`mqc.md`](./mqc.md)), processa em tempo real três tipos de dado de mercado distintos e os distribui para consumidores internos e para um frontend de trading via WebSocket.

### 1.1 Objetivos primários

1. **Performance máxima sustentada**: pipeline projetado para 100k msg/s em regime normal e 200k+ msg/s em pico, sem perda.
2. **Robustez contra desconexão**: a Cedro derruba clientes lentos quando a fila não-consumida no socket excede ~500.000 mensagens. Esse limite domina o design do reader.
3. **Diferenciação por tipo de dado**: quotes, books e trades têm semânticas e durabilidade distintas — cada um tem seu próprio pipeline e storage.
4. **Distribuição eficiente**: milhares+ de clientes WebSocket simultâneos consumindo updates em tempo real, com snapshot inicial completo na subscrição.

### 1.2 Não-objetivos (v1)

- Alta disponibilidade ativa-ativa (v2).
- Indicadores derivados em tempo real (médias móveis, VWAP, RSI etc.) — v1 só repassa dados crus.
- Backtesting/replay como produto (a raw archive permite no futuro).
- Multi-tenant com permissões finas por ativo — auth de v1 é apenas token de sessão.

---

## 2. Constraints Críticas

Estas constraints moldaram cada decisão de design. Qualquer mudança que viole uma delas precisa ser explicitamente justificada.

### 2.1 Limite de 500k mensagens na fila do socket Cedro

A Cedro **monitora ativamente a fila de mensagens não-consumidas pelo cliente**. Se passar de ~500k, o cliente é desconectado. Não há aviso prévio.

**Consequências de design:**
- O reader thread é sagrado. Tudo que existe no pipeline foi desenhado para garantir que a thread que drena bytes do socket **nunca seja deschedulada** ou bloqueada.
- Métrica de saúde primária do sistema: bytes pendentes no kernel recv buffer (`ioctl FIONREAD`).

### 2.2 Conexão única

O plano contratado com a Cedro suporta as ~100k subscriptions necessárias em uma única conexão. Não há sharding entre múltiplas conexões — toda a pressão de I/O fica no mesmo socket.

### 2.3 Single-writer per ticker no book

Mensagens do livro de ofertas (`BQT`) precisam ser aplicadas na ordem exata em que chegam, por ativo. Concorrência no estado do book quebra invariantes (posições erradas, ordens órfãs).

### 2.4 Perda zero de trades

Histórico de negócios alimenta análises críticas. Não pode perder mensagens em caso de crash ou reinicialização.

### 2.5 Hardware dedicado

Servidor bare-metal com 32+ cores físicas, 64+ GB RAM, NVMe, NIC 10GbE. Permite CPU pinning, `isolcpus` e tuning de kernel.

---

## 3. Stack

| Camada | Tecnologia | Justificativa |
|--------|-----------|---------------|
| Linguagem | Rust (stable, edition 2024) | Zero GC, performance comparável a C++, ergonomia muito superior. Único candidato realista para 200k msg/s sustentado com estado mutável complexo. |
| Allocator global | `tikv-jemallocator` (jemalloc) | 10-30% mais throughput em workloads concorrentes vs glibc malloc. Padrão em prod Rust (TiKV, Discord). Drop-in via `#[global_allocator]`. |
| Runtime async | `tokio` multi-thread | Padrão de facto, ecossistema gigante. Mas reader **não** usa tokio (ver 5.1). Glommio (thread-per-core + io_uring) deixado como upgrade futuro documentado caso vejamos saturação em produção. |
| Framing | `bytes` + `memchr` (SIMD) | Slices zero-copy + busca de delimitadores `\n`/`!` em ~30 GB/s. |
| Parsing | `nom` ou parser hand-rolled | Combinators ergonômicos; partes hot podem ir pra hand-rolled. |
| Filas internas | `crossbeam-channel` (MPMC) | Lock-free, bounded, melhor performance que `tokio::mpsc` quando não há cross-runtime. |
| Estado | `dashmap` (com `ahash` como BuildHasher) | Hashmap shardado lock-free para quotes. `ahash`/`foldhash` é 2-4× mais rápido que SipHash padrão (sem risco de hash-flooding em chaves internas). |
| Persistence (trades) | `sqlx` + Postgres COPY binário | TimescaleDB já operacional. Inserts via COPY são 50-100× mais rápidos que INSERT. |
| WAL trades | append-only file com fsync periódico | Mais simples e previsível que `sled` pra padrão append-only puro. |
| Snapshot cache | Redis (com `fred` ou `redis` crate) | Servir snapshot inicial pros api-servers sem bater no engine principal. **Upgrade futuro:** DragonflyDB (Redis-compatible, multi-threaded, 25× throughput) se Redis virar gargalo. **Decisão futura adicional:** migrar pra snapshot in-process por réplica do api-server (alimentado por NATS JetStream replay) se rodarmos 3+ réplicas. |
| Broker | `async-nats` (NATS JetStream) | Fanout escalável; api-server fica stateless e escala horizontal. JetStream usado pela retenção curta (útil pra reconectar clientes WS sem perder gap pequeno). |
| API server | `axum` + `tokio-tungstenite` | Padrão maduro no ecossistema Rust. Mesmo binário expõe `/ws` (streaming) e `/v1/*` (REST). |
| Protocolo WS/HTTP | `rmp-serde` (MessagePack) padrão; JSON opcional via `Accept` header | MessagePack ~50% do tamanho de JSON, 5-10× mais rápido. JSON disponível pra debug e clientes simples. |
| Raw archive | `zstd` + arquivo append-only rotativo | Compressão alta com baixo CPU. |
| CPU pinning | `core_affinity` | Garantia de que reader/splitter não migram de core. |
| Hashing rápido | `ahash` ou `foldhash` | BuildHasher pra HashMaps internos não-DoS-sensíveis. |
| Métricas | `metrics` + `metrics-exporter-prometheus` | Prometheus padrão. |
| Tracing | `tracing` + `tracing-subscriber` (JSON) | Logs estruturados pra agregação. **Atenção:** proibido no hot path (ver 5.8). |
| Errors | `thiserror` + `anyhow` | Padrão Rust. |
| Config | `figment` (TOML + env) | Layering de config. |

### 3.1 Armazenamento

| Tipo de dado | Storage | Durabilidade | Latência alvo (ingest → visível) |
|--------------|---------|--------------|----------------------------------|
| Quote (SQT) | In-process `DashMap` + Redis snapshot replicado | Descartável (último update vale) | < 5 ms p99 |
| Book (BQT) | In-process por ticker + Redis snapshot replicado | Reconstrutível ao reconectar | < 10 ms p99 |
| Trade (GQT) | WAL local + TimescaleDB hypertable | **Perda zero** (WAL antes do flush) | < 100 ms (em buffer) |
| Raw bruto | Arquivo zstd rotativo por hora | Cópia integral, retention configurável | N/A (assíncrono) |

---

## 4. Topologia em Alto Nível

```
                              ┌──────────────────────┐
                              │   Cedro Crystal :81  │
                              └──────────┬───────────┘
                                         │ (1 TCP conn)
        ╔════════════════════════════════▼══════════════════════════════════════╗
        ║                          INGEST DAEMON                                ║
        ║                          (binário 1)                                  ║
        ║                                                                       ║
        ║  ┌─────────────────────────────────────────────────────────────────┐  ║
        ║  │  std::thread Reader (core isolado)                              │  ║
        ║  │  socket bloqueante + SO_RCVBUF 128MB + TCP_NODELAY              │  ║
        ║  │  recv() em chunks de 64KB → ring BytesMut pre-alocado           │  ║  
        ║  └────────────────────────────┬────────────────────────────────────┘  ║
        ║                               │ Bytes (zero-copy)                     ║
        ║  ┌────────────────────────────▼────────────────────────────────────┐  ║
        ║  │  std::thread Frame Splitter (core isolado)                      │  ║
        ║  │  memchr2(\n, !) SIMD → frames brutos                            │  ║
        ║  ├────────────┬────────────────────────────────────────────────────┤  ║
        ║  │     ↓ tee  │                                                    │  ║
        ║  │  ┌─────────▼─────────┐                                          │  ║ 
        ║  │  │ Raw Archive       │                                          │  ║
        ║  │  │ zstd rotativo/h   │ → /var/data/raw/2026-05-26T14.zst        │  ║
        ║  │  └───────────────────┘                                          │  ║
        ║  └────────────────────────────┬────────────────────────────────────┘  ║
        ║                               │                                       ║
        ║   ┌───────────────────────────▼────────────────────────────────┐      ║
        ║   │       crossbeam MPMC bounded channel (capacidade 2M)       │      ║
        ║   └───────────────────────────┬────────────────────────────────┘      ║
        ║                               │                                       ║
        ║   ┌───────────────────────────▼─────────────────────────────────┐     ║
        ║   │   Parser Pool (N workers, tokio tasks em cores 2..M)        │     ║
        ║   │   parse(frame) → ProtocolMessage tipado                     │     ║
        ║   │   dispatch por hash(ticker) → engine apropriado             │     ║
        ║   └─┬──────────────────┬──────────────────┬─────────────────────┘     ║
        ║     │ Quote            │ Book              │ Trade                    ║
        ║     ▼                  ▼                   ▼                          ║
        ║  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────────┐      ║
        ║  │ QuoteEngine  │  │ BookEngine      │  │ TradeBuffer          │      ║
        ║  │ DashMap      │  │ Shards single-  │  │ WAL append + batch   │      ║
        ║  │ <Ticker,     │  │ writer (k cores)│  │ flush 5k/100ms       │      ║
        ║  │  QuoteState> │  │ ações + opções  │  │ todos os mercados    │      ║
        ║  │ todos os mkt │  │ Bovespa apenas  │  │                      │      ║
        ║  └──────┬───────┘  └────────┬────────┘  └──────────┬───────────┘      ║
        ║         │                    │                      │                 ║
        ║         └──────────┬─────────┴─────────────┐        │                 ║
        ║                    ▼                       ▼        ▼                 ║
        ║      ┌────────────────────────┐  ┌──────────────────────────┐         ║
        ║      │ Redis snapshot writer  │  │ NATS JetStream Publisher │         ║
        ║      │ market:quote:{ticker}  │  │ market.quote.{ticker}    │         ║
        ║      │ market:book:{ticker}   │  │ market.book.{ticker}     │         ║
        ║      │ (coalescer 100ms book) │  │ market.trade.{ticker}    │         ║
        ║      └────────────────────────┘  └──────────────┬───────────┘         ║
        ║                                                  │                    ║
        ║                                  ┌───────────────▼────────┐           ║
        ║                                  │ TimescaleDB COPY bin   │           ║ 
        ║                                  │ hypertable trades      │           ║
        ║                                  └────────────────────────┘           ║
        ╚═══════════════════════════════════════╤═══════════════════════════════╝
                                  Redis snapshots  │  NATS subjects
                                         (HTTP/WS)  │   (WS streaming)
                                                ▼   ▼
        ╔══════════════════════════════════════════════════════════════════════╗
        ║            API SERVER (binário 2 — N réplicas atrás de LB)           ║
        ║  axum + tokio-tungstenite — duas superfícies, mesma fonte:           ║
        ║                                                                      ║
        ║   /ws   (streaming)         /v1/quote/{ticker}, /v1/book/{ticker}    ║
        ║   ├─ multi-subscribe        /v1/trades/{ticker}, /v1/instruments     ║
        ║   ├─ snapshot Redis         GET → Redis → JSON/MessagePack           ║
        ║   └─ stream via NATS        ETag + Cache-Control + rate limit        ║
        ║                                                                      ║
        ║   Auth (token) + content negotiation (JSON|MessagePack) comuns       ║
        ╚══════════════════════════════════════════════════════════════════════╝
```

---

## 5. Componentes Detalhados

### 5.1 Reader (`cedro-client::connection::Reader`)

**Função única e exclusiva**: drenar bytes do socket TCP o mais rápido possível.

- **Thread:** `std::thread::Builder::new().spawn(...)`. **Não** é uma tokio task. Tokio pode descheedular tasks se outras dominarem o runtime; não é aceitável para o reader.
- **CPU pinning:** core isolado via `isolcpus=N` no boot do kernel. `core_affinity::set_for_current(core)` na entrada da thread. O core não recebe IRQs (`irqbalance` configurado para evitar).
- **Socket:** `socket2::Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))`, modo bloqueante (não-async).
  - `SO_RCVBUF = 128 MB` (requer `sysctl -w net.core.rmem_max=134217728`). Aumentado vs 64MB inicial porque o limite Cedro de 500k msgs × ~200B/msg ≈ 100MB — buffer kernel **precisa** caber a margem completa, senão enchemos o kernel buf antes do limite Cedro e a contagem dos 500k começa silenciosamente.
  - `TCP_NODELAY = 1` (Cedro envia em bursts pequenos; sem Nagle).
  - `TCP_QUICKACK = 1`.
- **Loop:**
  ```rust
  // pseudo-Rust
  let mut ring = BytesMut::with_capacity(8 * 1024 * 1024);
  loop {
      ring.reserve(64 * 1024);
      let n = socket.read(&mut buf[..])?;
      if n == 0 { return Err(Eof); }
      ring.put_slice(&buf[..n]);
      while let Some(frame) = try_split_frame(&mut ring) {
          tx.send(frame)?;  // crossbeam channel, bounded mas grande
      }
  }
  ```
- **Zero alocação no hot path:** `Bytes::from(ring.split_to(idx))` produz slices que compartilham o buffer.

#### 5.1.1 Handshake (uma vez por conexão)

Conforme [`api.md`](./api.md) seção 2: Software Key (linha vazia se ausente) → Username → Password → aguardar `You are connected`. Timeout 10s por prompt.

#### 5.1.2 Watchdog do kernel buffer

Thread separada (também pinada, prioridade baixa) que executa a cada 100ms:

```rust
let mut pending: c_int = 0;
ioctl(socket_fd, FIONREAD, &mut pending);
gauge!("cedro_kernel_recv_buf_bytes").set(pending as f64);
```

**Limiares (mais agressivos que o intuitivo — explicação abaixo):**
- `> 20%` do `SO_RCVBUF` (≈ 26MB / 128k msgs) → warning logado, alarme Prometheus, snapshot de diagnóstico (queue depths internos, throughput parser).
- `> 40%` (≈ 51MB / 256k msgs, **metade do limite Cedro**) → ação imediata: encerrar processo limpo (`process::exit(2)` após drenar buffer trade, flush WAL, fechar logs).

**Por que tão conservador:** quando o kernel buffer enche, TCP envia zero-window pra Cedro, que vê o cliente como lento e começa a contar pros 500k. Ou seja, kernel buffer cheio ≈ início da contagem Cedro. Acima de 40% já estamos com metade do orçamento Cedro consumido. Melhor encerrar voluntariamente e reconectar do que ser derrubado (Cedro pode impor cooldown ou marcar o usuário como problemático).

### 5.2 Frame Splitter

Thread dedicada e pinada em outro core isolado. Consome do reader (ou compartilha o ring buffer com mutex sem contenção, se mantivermos arquitetura single-producer-single-consumer).

- Usa `memchr::memchr2(b'\n', b'!', ...)` (AVX2).
- Cada frame extraído é um `Bytes` (slice zero-copy).
- **Tee:** cada frame é também encaminhado pra raw archive (canal separado, slow consumer não pode bloquear o frame channel).

### 5.3 Raw Archive

Thread dedicada que:
- Recebe frames brutos por channel separado.
- Buffer interno de ~1 MB.
- Quando buffer cheio ou a cada 1s, comprime com `zstd` (level 3) e escreve no arquivo da hora.
- Arquivo nomeado: `/var/data/raw/{YYYY-MM-DD}T{HH}.zst`.
- Rotaciona ao virar a hora.
- Retention configurável (padrão 90 dias).
- I/O nunca bloqueia o pipeline: se o consumer for lento, o channel pode descartar com métrica explícita (`raw_archive_dropped_frames_total`) — preferir perder archive a perder a Cedro.

### 5.4 Parser Pool

Tokio runtime multi-thread com N workers (default: `num_cpus - 4`, reservando cores pra reader, splitter, watchdog e SO).

Cada worker:
1. `recv()` do crossbeam channel.
2. Identifica cabeçalho (`T:`, `B:`, `Z:`, `V:`, `O:`, `GNA:`, `VAP:`, `GTC:`, `C:`, `G:`, `E:`).
3. Aplica parser específico → `ProtocolMessage` tipada.
4. Dispatch por `hash(ticker) % NUM_ENGINE_SHARDS` → manda pra fila do shard correto.

### 5.5 Engines

#### 5.5.1 QuoteEngine

- `DashMap<Ticker, Arc<QuoteState>>`. `DashMap` é shardada internamente, lock-free na leitura, lock-per-shard na escrita.
- `QuoteState` é um struct com todos os 160+ campos do SQT (`Option<T>` nos que podem não vir).
- Update aplica diff por índice: parser produz `Vec<(u16, QuoteValue)>` e engine faz `state.apply_diff(&diff)`.
- Após apply, publica em NATS (`market.quote.{ticker}`) e atualiza Redis (`HSET market:quote:{ticker}`) com coalescing de 50ms por ticker.

#### 5.5.2 BookEngine

**Escopo: apenas tickers retornados por `MQC Bovespa 1` (ações) e `MQC Bovespa 2` (opções).**

Justificativa: futuros têm market makers que inflam BQT em ordem de magnitude. Excluí-los do BQT torna a operação viável em 100k ativos. Quotes e trades continuam universais.

- Shards de book: `N_BOOK_SHARDS` workers (default 8), cada um possuindo um subset de tickers via `hash(ticker) % N_BOOK_SHARDS`.
- **Sempre o mesmo ticker → o mesmo shard worker**. Single-writer garantido. Sem locks, sem races.
- Estrutura por ticker:
  ```rust
  struct OrderBook {
      bids: Vec<Order>,         // posicional, conforme BQT
      asks: Vec<Order>,
      by_order_id: HashMap<OrderId, (Side, usize)>,  // lookup rápido
  }
  ```
- Operações BQT:
  - `A` (add): inserir em `bids[pos]` ou `asks[pos]`, deslocar resto.
  - `U` (update): remover de `posição_antiga`, inserir em `posição_nova`.
  - `D:1`: remover posição específica.
  - `D:2`: remover de 0 até posição (inclusive).
  - `D:3`: limpar AMBOS os lados (`bids.clear(); asks.clear(); by_order_id.clear()`).
- Cada update: emite `BookDelta { ticker, ops: Vec<Op> }` para NATS e atualiza snapshot Redis (coalescer 100ms — book é mais pesado de serializar).

#### 5.5.3 TradeBuffer

**Constraint: perda zero.**

- Cada trade chega → escreve em WAL local (`sled` ou arquivo append `.wal` com fsync periódico — TBD em prototipagem).
- Em paralelo: adiciona ao buffer in-memory.
- A cada 100ms **ou** quando buffer atinge 5.000 trades: flush em batch para TimescaleDB via `COPY trades FROM STDIN WITH BINARY`.
- Após `COMMIT` do COPY: marca offset no WAL como flushed.
- Em startup: lê WAL desde o último offset flushed e reprocessa.
- WAL truncado quando idade > 24h e tudo flushed.

### 5.6 Fanout (NATS)

- `async-nats` cliente único reusado por todas as tasks.
- **Subjects:**
  - `market.quote.{ticker}` — payload: `QuoteSnapshot` ou `QuoteDelta` em MessagePack.
  - `market.book.{ticker}` — payload: `BookSnapshot` ou `BookDelta` em MessagePack.
  - `market.trade.{ticker}` — payload: `Trade` em MessagePack.
- **JetStream:** stream `MARKET` com retention `Limits` (max 1h, max 1GB) — não somos source-of-truth via NATS, apenas distribuímos.
- **Coalescing:** publisher mantém timer 50ms (quote) / 100ms (book). Updates múltiplos no mesmo ticker dentro da janela viram um único publish com estado consolidado.

### 5.7 API Server (binário separado — WS + HTTP)

Componente que serve consumidores externos. **Mesmo binário expõe duas superfícies:**

- **WebSocket** (`/ws`): streaming de longa duração com subscribe/unsubscribe dinâmico. Default pra UIs de trading e bots que precisam de updates em tempo real.
- **HTTP REST** (`/v1/*`): polling pontual, snapshots por requisição. Default pra dashboards estáticos, exportações, scripts, integrações com sistemas que não suportam WS.

Ambas as superfícies leem da **mesma fonte**: o Redis snapshot atualizado pelo `ingest-daemon`. Isso garante consistência absoluta entre os dois caminhos: o snapshot inicial do WS e o GET HTTP retornam exatamente o mesmo payload do mesmo timestamp lógico.

> **Sobre "diretamente da memória":** o Redis nessa arquitetura É a representação em memória do estado. O `ingest-daemon` empurra updates de quote com coalescing de 50ms e snapshots de book com coalescing de 100ms. Latência total cliente → Redis → cliente é ~1-3 ms em rede privada — funcionalmente equivalente a ler do estado in-process do daemon, mas com a vantagem decisiva de **escala horizontal** (N réplicas do api-server atrás de LB).

#### 5.7.1 Auth (comum a WS e HTTP)

- Header `Authorization: Bearer <token>` em ambos os transportes (no WS: validado no upgrade handshake).
- Validação contra serviço externo (TBD — pode ser HTTP call cacheado, JWT local, ou Redis lookup).
- Rate limiting por token: 100 req/s default em HTTP; sem limite explícito no WS (controlado por backpressure).

#### 5.7.2 WebSocket (`/ws`)

Conexão persiste indefinidamente. Cliente pode multiplexar múltiplos canais e tickers no mesmo WS.

**Cliente → server** (MessagePack):
```jsonc
{ "op": "sub", "channel": "quote" | "book" | "trade", "ticker": "PETR4" }
{ "op": "unsub", "channel": "quote", "ticker": "PETR4" }
{ "op": "ping", "ts": 1234567890 }
```

**Server → cliente**:
```jsonc
// ack de subscribe com snapshot inicial
{ "op": "snap", "channel": "quote", "ticker": "PETR4", "data": { ...QuoteState } }
{ "op": "snap", "channel": "book",  "ticker": "PETR4", "data": { "bids":[...], "asks":[...] } }
{ "op": "snap", "channel": "trade", "ticker": "PETR4", "data": [ ...últimos 50 trades ] }
// updates de stream
{ "op": "upd",  "channel": "quote", "ticker": "PETR4", "data": { "2": 32.50, "9": 1500000 } }
{ "op": "upd",  "channel": "book",  "ticker": "PETR4", "data": { "ops": [...] } }
{ "op": "upd",  "channel": "trade", "ticker": "PETR4", "data": { ...Trade } }
// erros
{ "op": "err",  "code": "BOOK_NOT_AVAILABLE", "ticker": "WDOZ25", "msg": "book not available for futures" }
```

**Fluxo de subscribe WS:**
1. Validar canal+ticker (book negado para tickers fora de ações/opções Bovespa).
2. `GET market:{channel}:{ticker}` no Redis → snapshot.
3. Enviar `snap` ao cliente.
4. `nats.subscribe("market.{channel}.{ticker}")` → forward cada mensagem como `upd`.

**Backpressure WS:** se `send_buffer` do cliente atingir limite, desconectar (cliente lento não pode degradar o broadcast).

#### 5.7.3 HTTP REST (`/v1/*`)

Padrão: `Content-Type: application/json`. Negotiation via `Accept: application/msgpack` retorna MessagePack pra clientes performance-sensitive.

| Método | Path | Descrição |
|--------|------|-----------|
| `GET` | `/v1/quote/{ticker}` | Snapshot do quote — equivalente ao `snap` WS |
| `GET` | `/v1/quote?tickers=A,B,C` | Batch quote (até 500 tickers em uma req) |
| `GET` | `/v1/book/{ticker}` | Snapshot do book — só ações/opções Bovespa, 404 caso contrário |
| `GET` | `/v1/book?tickers=A,B,C` | Batch book (até 500 tickers) |
| `GET` | `/v1/trades/{ticker}?limit=N` | Últimos N trades (default 50, max 1000) — lê do Redis |
| `GET` | `/v1/trades/{ticker}?since=ISO8601&limit=N` | Range histórico — lê do TimescaleDB |
| `GET` | `/v1/instruments?market=Bovespa&type=1` | Lista cacheada do MQC (descobrir universo) |
| `GET` | `/v1/brokers?market=BOVESPA` | Lista de corretoras do GPN |
| `GET` | `/v1/health` | Status do api-server (liveness/readiness) |

**Convenções HTTP:**

- **Caching:** resposta inclui `Cache-Control: max-age=1` + `ETag` baseado em hash do payload. Cliente pode usar `If-None-Match` → `304 Not Modified`.
- **Errors:** formato padrão `{ "code": "...", "message": "...", "ticker": "..." }`. Status codes: `400` (bad request), `401` (no token), `403` (token inválido), `404` (ticker desconhecido ou book indisponível), `429` (rate limit), `503` (api-server iniciando ou Redis down).
- **Bulk batching:** `GET /v1/quote?tickers=A,B,C` retorna `{ "PETR4": {...}, "VALE3": {...}, "ITUB4": null }` — `null` para tickers desconhecidos. Não falha o batch inteiro por causa de um ticker ruim.
- **Paginação trades históricos:** `GET /v1/trades/{ticker}?since=...&limit=1000&cursor=<id>` — cursor opaco baseado em (occurred_at, trade_id).
- **CORS:** lista de origens permitidas via config (`api_server.cors.allowed_origins`). Default permissivo em dev, restritivo em prod.
- **Compression:** `Accept-Encoding: gzip` suportado pra payloads de batch grandes.

**Fluxo HTTP típico:**

```
GET /v1/quote/PETR4
  ↓
api-server valida token (lookup cacheado)
  ↓
api-server: GET market:quote:PETR4 no Redis (~0.5ms)
  ↓
deserializa, serializa em JSON, computa ETag
  ↓
200 OK + body + ETag header
```

**Endpoint de trades histórico (único que pode bater no Timescale):**

```
GET /v1/trades/PETR4?since=2026-05-26T10:00:00Z&limit=500
  ↓
api-server valida token + parametros
  ↓
SELECT * FROM trades
WHERE ticker = 'PETR4' AND occurred_at >= '2026-05-26T10:00:00Z'
ORDER BY occurred_at ASC, trade_id ASC
LIMIT 500
  ↓
200 OK + body + (cursor pra próxima página se aplicável)
```

Esse é o único endpoint que toca disco — todos os outros são puramente in-memory via Redis.

### 5.8 Logging Discipline (regra dura do projeto)

**Proibido logar no hot path.** O hot path inclui: reader thread, frame splitter, parser pool, engine workers, NATS publisher, Redis snapshot writer.

**Por quê:** a 200k msg/s, cada `tracing::trace!` ou `tracing::debug!` — mesmo desabilitado em runtime via filter — tem custo de avaliação de argumentos, format string, lock no global subscriber e potencialmente alocação. Em loop apertado, isso solo destrói throughput e pode levar à derrubada pela Cedro ([§2.1](#21-limite-de-500k-mensagens-na-fila-do-socket-cedro)).

**Política concreta:**

- **Hot path usa exclusivamente métricas Prometheus** (`counter!`, `gauge!`, `histogram!`). Métricas são lock-free e têm overhead constante de ~10ns.
- **Logs `info!` / `warn!` / `error!` permitidos apenas em:** boot, reconnect, shutdown, erros raros (ex: parser falha em um frame específico — log com sample 1:1000).
- **`tracing::trace!` e `tracing::debug!` ficam atrás de `#[cfg(debug_assertions)]`** — em release, são otimizados pra zero. Isso significa: em release **não há** tracing fino no caminho hot. Pra debug profundo em prod, usa flamegraphs e perf, não logs.
- **Code review enforces:** PR que adiciona log no hot path é rejeitado.
- **Lint:** considerar `clippy::disallowed_macros` configurado pra alertar usos de `tracing::*` em módulos hot.

Erros de parsing que ocorrem com frequência (ex: enum desconhecido) viram contador (`parser_errors_total{kind="..."}`), não log. Análise post-hoc usa o raw archive ([§5.3](#53-raw-archive)).

---

## 6. Estrutura do Workspace

```
market_data_v2/
├── Cargo.toml                    # workspace root
├── ARCHITECTURE.md               # este documento
├── api.md                        # docs Cedro principal
├── mqc.md                        # docs Cedro adendo MQC/GPN
├── README.md
│
├── config/
│   ├── default.toml
│   └── production.toml
│
├── crates/
│   ├── cedro-protocol/           # parsing puro, sem I/O
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── frame.rs          # framing \n e !
│   │       ├── types.rs          # ProtocolMessage, QuoteState, BookOp, Trade
│   │       ├── enums.rs          # MarketCode, AssetType, Status, Phase, ...
│   │       └── parser/
│   │           ├── mod.rs
│   │           ├── sqt.rs        # T:<ticker>:<time>:<idx>:<val>:...!
│   │           ├── bqt.rs        # B:<ticker>:A|U|D|E:...
│   │           ├── sab.rs        # Z:<ticker>:...
│   │           ├── gqt.rs        # V:<ticker>:...
│   │           ├── nem.rs        # O:...
│   │           ├── vap.rs        # VAP:...
│   │           ├── gtc.rs        # GTC:...
│   │           ├── mqc.rs        # C:<MERCADO>:<ticker> | C:<MERCADO>:E
│   │           ├── gpn.rs        # G:<exchange>:<code>:<name>:<cedro_id>:<active>
│   │           └── error.rs      # E:<code>:...
│   │
│   ├── cedro-client/             # conexão TCP, handshake, comandos, reader
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── connection.rs     # handshake, std::thread reader
│   │       ├── watchdog.rs       # ioctl FIONREAD loop
│   │       ├── reconnect.rs      # política tabela 10.1
│   │       ├── subscription.rs   # estado das subs ativas (resub em reconnect)
│   │       └── commands.rs       # SQT, BQT, SAB, GQT, MQC, GPN, USQ, ...
│   │
│   ├── market-engine/            # estado e processamento
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── shard.rs          # routing hash(ticker) → shard
│   │       ├── quote.rs          # QuoteEngine, QuoteState diff/apply
│   │       ├── book.rs           # BookEngine, single-writer workers
│   │       ├── order_book.rs     # OrderBook struct + ops
│   │       ├── trade.rs          # TradeBuffer + WAL + batch flush
│   │       └── wal.rs            # write-ahead log local
│   │
│   ├── persistence/              # TimescaleDB + Redis
│   │   ├── Cargo.toml
│   │   ├── migrations/
│   │   │   └── 001_trades_hypertable.sql
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── timescale.rs      # pool + COPY binário
│   │       └── redis_snapshot.rs # writer com coalescing
│   │
│   ├── raw-archive/              # zstd rotativo
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       └── writer.rs
│   │
│   ├── fanout/                   # NATS publisher
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── publisher.rs
│   │       └── coalescer.rs
│   │
│   ├── api-server/               # axum: WS + HTTP REST (lib usada pelo bin)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── router.rs         # construção do axum::Router (WS + HTTP)
│   │       ├── auth.rs           # validação de token (comum a WS e HTTP)
│   │       ├── ws/
│   │       │   ├── mod.rs
│   │       │   ├── handler.rs    # /ws endpoint
│   │       │   ├── session.rs    # estado por conexão WS
│   │       │   └── nats_bridge.rs# NATS subscribe → WS forward
│   │       ├── http/
│   │       │   ├── mod.rs
│   │       │   ├── quote.rs      # GET /v1/quote/* (single + batch)
│   │       │   ├── book.rs       # GET /v1/book/* (single + batch)
│   │       │   ├── trades.rs     # GET /v1/trades/* (Redis recents + Timescale history)
│   │       │   ├── instruments.rs# GET /v1/instruments
│   │       │   ├── brokers.rs    # GET /v1/brokers
│   │       │   ├── health.rs     # GET /v1/health
│   │       │   ├── content_neg.rs# JSON vs MessagePack
│   │       │   ├── etag.rs       # ETag + If-None-Match
│   │       │   └── rate_limit.rs # rate limiter por token
│   │       └── snapshot_store.rs # leitor unificado de Redis (usado por WS e HTTP)
│   │
│   └── observability/            # tracing + metrics
│       ├── Cargo.toml
│       └── src/
│           └── lib.rs
│
└── bin/
    ├── ingest-daemon/            # binário principal
    │   ├── Cargo.toml
    │   └── src/
    │       └── main.rs
    └── api-server/               # binário separado pra escalar (WS + HTTP)
        ├── Cargo.toml
        └── src/
            └── main.rs
```

---

## 7. Fluxo de Bootstrap (Ingest Daemon)

Sequência canônica de boot — ordem importa:

```
1. Carregar config + inicializar tracing/metrics
2. Conectar TimescaleDB + Redis + NATS (fail fast se algum cair)
3. Iniciar thread reader Cedro + handshake
4. GPN BOVESPA
   ├─ coletar linhas G:... até primeira linha não-G: ou timeout
   └─ popular dicionário cedro_id → broker info
5. MQC em série (espaçados ~50ms, timeout 20s/linha):
   ├─ MQC Bovespa 1   (ações)
   ├─ MQC Bovespa 2   (opções)
   ├─ MQC Bovespa 3   (índices)
   ├─ MQC Bovespa 10  (fracionário)
   ├─ MQC Bovespa 13  (ETFs)
   ├─ MQC Bovespa 20  (corp)
   ├─ MQC BMF T 7 S 130, 60, 50, 40, 30, 20  (futuros)
   └─ MQC BMF T 2 S 150 (opções sobre futuros)
6. Dedup + cache em disco (TTL = próximo pregão)
7. Calcular set de book: ações ∪ opções Bovespa
8. Lotear subscriptions (~500/s pra não estourar o buffer):
   ├─ SQT <ticker>  para TODO o universo
   ├─ BQT <ticker>  apenas pra book_set
   └─ GQT <ticker> S 50  para TODO o universo
9. Aguardar primeiros snapshots chegarem (heurística: 80% dos tickers responderam)
10. Marcar /health como "ready"
11. Loop principal: tokio runtime processando o pipeline indefinidamente
```

Sinais:
- `SIGTERM`: drenar trade buffer + flush WAL + fechar Cedro → exit(0).
- `SIGUSR1`: dump de métricas detalhadas pra log.

---

## 8. Schema TimescaleDB

```sql
-- crates/persistence/migrations/001_trades_hypertable.sql

CREATE TABLE IF NOT EXISTS trades (
    ticker        TEXT       NOT NULL,
    trade_id      BIGINT     NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL,
    price         NUMERIC(18, 6) NOT NULL,
    quantity      INTEGER    NOT NULL,
    broker_buy    INTEGER,
    broker_sell   INTEGER,
    aggressor     CHAR(1)    NOT NULL,     -- 'A' | 'V' | 'I'
    trade_cond    SMALLINT   NOT NULL,     -- enum 4.1.5 condição
    trade_cond_orig TEXT,                  -- lista space-separated (raw)
    operation     CHAR(1)    NOT NULL,     -- 'A' | 'D' | 'R'
    PRIMARY KEY (ticker, occurred_at, trade_id)
);

SELECT create_hypertable('trades', 'occurred_at',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trades_ticker_time
    ON trades (ticker, occurred_at DESC);

-- compression policy: comprimir chunks após 1 dia
ALTER TABLE trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ticker'
);
SELECT add_compression_policy('trades', INTERVAL '1 day');

-- retention: trades brutos por 2 anos (ajustável)
SELECT add_retention_policy('trades', INTERVAL '2 years');

-- continuous aggregates pra candles (exemplo 1m)
CREATE MATERIALIZED VIEW IF NOT EXISTS trades_candle_1m
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('1 minute', occurred_at) AS bucket,
    first(price, occurred_at)  AS open,
    max(price)                  AS high,
    min(price)                  AS low,
    last(price, occurred_at)   AS close,
    sum(quantity)               AS volume,
    count(*)                    AS trades
FROM trades
GROUP BY ticker, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('trades_candle_1m',
    start_offset => INTERVAL '2 hours',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');
```

Candles 5m, 15m, 1h e 1d podem ser materializados sobre `trades_candle_1m` em vez de sobre `trades` para reduzir custo.

---

## 9. Reconexão e Tratamento de Erros

Conforme `api.md` seção 10.1:

| Código Cedro | Ação |
|--------------|------|
| `E:1, E:4, E:5, E:10, E:13, E:14` (bug no cliente) | Log + alerta, não retry. Fail fast. |
| `E:3, E:9, E:17, E:18, E:19` (permissão) | Não retry. Sinaliza para o operador. |
| `E:11, E:15` (servidor temporário) | Retry com backoff exponencial 1→60s. |
| `E:12` (mudança de host) | Atualizar host config e reconectar imediatamente. |
| `E:6, E:7, E:8` (desconexão forçada) | **Não reconectar automaticamente**: pode haver outra sessão ativa. Requer intervenção. |
| `E:2, E:16` (objeto inexistente) | Sinalizar; não tentar reassinar. |

**Reconexão por timeout/queda do socket** (fora de erros explícitos):
- Backoff: 1s, 2s, 4s, 8s, 16s, 30s, 60s, 60s, ...
- Reaplica TODA a sequência de bootstrap.
- Métricas: `cedro_reconnects_total`, `cedro_reconnect_duration_seconds`.

---

## 10. Observabilidade

### 10.1 Métricas Prometheus (`/metrics`)

| Métrica | Tipo | Significado |
|---------|------|-------------|
| `cedro_bytes_read_total` | Counter | Bytes lidos do socket |
| `cedro_kernel_recv_buf_bytes` | Gauge | **Bytes em buffer kernel (crítico)** |
| `cedro_frames_total{type=...}` | Counter | Frames parseados por tipo |
| `cedro_reconnects_total{reason=...}` | Counter | Reconexões |
| `frame_channel_depth` | Gauge | Profundidade do channel reader→parser |
| `parser_messages_per_second` | Gauge | Throughput do parser |
| `parser_errors_total{kind=...}` | Counter | Erros de parsing |
| `book_apply_latency_seconds` | Histogram | Tempo de aplicar update no book |
| `book_levels_count{ticker=...}` | Gauge | Níveis no book por ticker (top 100 amostrados) |
| `trade_buffer_size` | Gauge | Trades pendentes no buffer |
| `trade_flush_duration_seconds` | Histogram | Latência do flush para Timescale |
| `wal_size_bytes` | Gauge | Tamanho do WAL local |
| `nats_publish_errors_total` | Counter | Falhas de publish NATS |
| `redis_snapshot_errors_total` | Counter | Falhas de write Redis |
| `raw_archive_dropped_frames_total` | Counter | Frames descartados do archive |
| `ws_connections_active` | Gauge | Conexões WS ativas |
| `ws_subscriptions_active{channel=...}` | Gauge | Subscriptions WS ativas |
| `ws_messages_sent_total{channel=...}` | Counter | Mensagens WS enviadas |
| `ws_client_disconnects_total{reason=...}` | Counter | Desconexões WS |
| `http_requests_total{endpoint=...,status=...}` | Counter | Requisições HTTP |
| `http_request_duration_seconds{endpoint=...}` | Histogram | Latência HTTP por endpoint |
| `http_rate_limited_total{token=...}` | Counter | Rejeições por rate limit |
| `redis_snapshot_get_duration_seconds` | Histogram | Latência leitura Redis (usada por WS e HTTP) |
| `timescale_query_duration_seconds{query=...}` | Histogram | Latência queries Timescale (`/v1/trades` histórico) |

### 10.2 Alertas críticos (Prometheus AlertManager)

- `cedro_kernel_recv_buf_bytes / SO_RCVBUF > 0.2` por 30s → warning
- `cedro_kernel_recv_buf_bytes / SO_RCVBUF > 0.4` instantâneo → critical, pagina (e o daemon vai se auto-encerrar — ver 5.1.2)
- `frame_channel_depth > 1_000_000` → critical
- `parser_errors_total` rate > 100/s → warning
- `trade_buffer_size > 50_000` → warning
- `cedro_reconnects_total` rate > 1/min → warning

### 10.3 Tracing

`tracing` com `EnvFilter` configurável. Output JSON estruturado para agregação. Trace spans nos pontos:
- handshake completo
- cada batch de MQC
- cada batch de subscriptions
- flush do trade buffer
- WS connection lifecycle

---

## 11. Capacidade e Dimensionamento

### 11.1 Memória esperada

| Componente | Estimativa |
|------------|------------|
| QuoteState (100k tickers × ~1 KB) | ~100 MB |
| OrderBook (~30k tickers ações+opções × 50 levels × 2 sides × ~120 B) | ~400 MB |
| Redis snapshots (100k × ~1 KB médio) | ~100 MB (no Redis, externo) |
| Frame channel buffer (2M × 24 B Bytes overhead) | ~50 MB |
| Ring buffer reader | 8 MB |
| Tokio task stacks, etc | ~200 MB |
| **Total esperado do ingest-daemon** | **~1-2 GB RSS** |

### 11.2 CPU esperada (servidor 32 cores)

| Core(s) | Workload |
|---------|----------|
| 0 |     Reader (isolado, `isolcpus`) |
| 1 |     Frame splitter (isolado) |
| 2 |     Watchdog + raw archive writer |
| 3 |     NATS publisher + Redis writer |
| 4-11 |  Parser pool (8 workers) |
| 12-19 | Book engine shards (8 workers) |
| 20-23 | Trade buffer + Timescale writer |
| 24-31 | Reservados pra SO, observability, surto |

### 11.3 Disco

- WAL trades: ~50 MB/h em regime normal, picos de 200 MB/h.
- Raw archive: ~1-3 GB/h comprimido. 30-90 GB/dia. Retention 90 dias = ~3-8 TB.
- TimescaleDB trades: depende da retenção; com compression policy 1d, ~10-50 GB/mês.

### 11.4 HA assimétrica (importante)

A v1 **não tem HA ativa-ativa**, mas tem uma assimetria importante a documentar:

| Componente | HA na v1? | Por quê |
|------------|-----------|---------|
| `ingest-daemon` | **Não — SPOF** | Cedro derruba 2ª conexão do mesmo usuário (`E:6` / `E:8`). Conexão única é constraint contratual. |
| `api-server` | **Sim por design** | Stateless (lê de Redis + NATS). N réplicas atrás de LB. Adicionar/remover é trivial. |
| Redis, NATS, TimescaleDB | Depende da infra | Configurações HA padrão de cada produto (Redis Sentinel/Cluster, NATS cluster, Timescale replicação). |

**Mitigação do SPOF do `ingest-daemon`:**

- **systemd com restart automático** — `Restart=always`, `RestartSec=5s`. Crash → re-spawn em segundos.
- **Healthchecks frequentes** (`/health` + verificação de queue depth + idade do último frame).
- **Raw archive ([§5.3](#53-raw-archive)) como recovery** — em caso de gap, é possível reprocessar do archive pra repopular Timescale e snapshots.
- **WAL trades garante perda zero** em crash limpo ou abrupto.
- **Reconnect com bootstrap completo** ao reconectar — re-emite todo o universo de subscriptions sem manual intervention.
- **MTBF realista:** com hardware bare-metal e SO estável, MTTR de minutos pra recovery completo é factível. MTBF de meses entre crashes não-planejados é o alvo.

**v2 (futuro):** active-passive com hot standby. Standby fica em "stand-down" Cedro (não conecta), mantém warm copy do Redis/NATS. Promoção manual ou automática (com cuidado pra não disparar 2 conexões simultâneas na Cedro).

### 11.5 Deployment Tuning Checklist

Checklist a aplicar quando provisionarmos o servidor bare-metal (não codamos nada aqui, é configuração de SO):

**Kernel / sysctl:**
- [ ] `net.core.rmem_max = 134217728` (128 MB)
- [ ] `net.core.wmem_max = 67108864` (64 MB — pros writes do NATS publisher)
- [ ] `net.core.netdev_max_backlog = 16384`
- [ ] `net.ipv4.tcp_rmem = 4096 87380 134217728`
- [ ] `net.ipv4.tcp_no_metrics_save = 1`
- [ ] `vm.swappiness = 1` (evitar swap-out de buffers críticos)
- [ ] `vm.overcommit_memory = 1`

**Boot kernel cmdline:**
- [ ] `isolcpus=0,1` (cores do reader e splitter isolados do scheduler geral)
- [ ] `nohz_full=0,1` (sem tick timer nesses cores)
- [ ] `rcu_nocbs=0,1` (RCU callbacks fora dos cores isolados)

**IRQ affinity:**
- [ ] Desabilitar `irqbalance` e configurar afinidade manual.
- [ ] IRQs da NIC fixadas em cores **não-isolados** (ex: cores 24-31), longe do reader.

**Memória:**
- [ ] Huge pages: `vm.nr_hugepages` proporcional ao working set (avaliar com `transparent_hugepage=madvise` no boot).
- [ ] **NUMA**: se servidor for multi-socket, `numactl --cpunodebind=N --membind=N` pra cada binário, escolhendo o nó NUMA do core do reader.
- [ ] `mlockall` no `ingest-daemon` (via `libc::mlockall(MCL_CURRENT | MCL_FUTURE)`).

**Tempo:**
- [ ] `chrony` ou `ntpd` sincronizado com servidor de tempo local de baixa stratum (idealmente o servidor NTP da B3 ou um GPS-disciplined NTP local).
- [ ] Drift máximo aceitável: < 10ms em regime normal.

**Filesystem:**
- [ ] WAL e raw archive em NVMe local (não em rede).
- [ ] `noatime` no mount.
- [ ] ext4 ou XFS; XFS preferível pra writes append-only grandes.

**systemd unit:**
- [ ] `Restart=always`, `RestartSec=5s`
- [ ] `LimitNOFILE=1048576`
- [ ] `LimitMEMLOCK=infinity` (pro `mlockall`)
- [ ] `CPUAffinity=` excluindo cores isolados (o binário lida com pinning fino internamente)

**Monitoramento da máquina:**
- [ ] `node_exporter` pra métricas do SO.
- [ ] Alerta em queda de NIC, throttling de CPU, swap usage > 0.

---

## 12. Segurança

- Credenciais Cedro: em arquivo `.env` ou vault, nunca commitadas. Carregadas via `figment`.
- Token de auth WS: validação em call HTTP/Redis. Tokens com expiração curta.
- TLS termination: nginx/caddy/traefik na frente do WS server. Ingest daemon nunca expõe porta pública.
- TimescaleDB: conexão via TLS, role com permissões mínimas (INSERT em `trades`, SELECT em continuous aggregates).
- Redis: senha ou ACL, rede interna isolada.
- NATS: NKey-based auth ou JWT.

---

## 13. Plano de Implementação (mapeado para tasks)

| Ordem | Crate / Componente | Task |
|-------|--------------------|------|
| 1 | `cedro-protocol` (parser puro testável isoladamente) | #4 |
| 2 | TimescaleDB schema | #2 |
| 3 | Workspace setup completo | #3 |
| 4 |  `cedro-client` (TCP + reader + handshake) | #5 |
| 5 |  `market-engine` (quote + book + trade) | #6 |
| 6 |  `raw-archive` | #7 |
| 7 |  `persistence` (TimescaleDB + Redis snapshot) | (parte de #2 e #8) |
| 8 |  `fanout` (NATS publisher) | #8 |
| 9 |  `observability` | #10 |
| 10 | `bin/ingest-daemon` (orquestrador) | #11 |
| 11 | `api-server` (WS + HTTP, lib + binário) | #9 |
| 12 | Stress tests + benchmarks | #12 |

Validações intermediárias:
- Após #4: parser passa em 100% dos fixtures derivados dos exemplos de `api.md` e `mqc.md`.
- Após #5: handshake real contra Cedro + recebimento de mensagens cruas, sem parsing ainda.
- Após #6: simulador de mensagens injeta 200k msg/s e o engine sustenta sem queue growth.
- Após #11: sistema completo conecta na Cedro, faz bootstrap, recebe stream estável por 1h sem erros.
- Após #12: validação de SLOs (latência p99, throughput, recovery time).

---

## 14. Roadmap pós-v1

- v1.1: HA active-passive (hot standby replicando snapshots via Redis).
- v1.2: Stream processor (crate dedicado consumindo NATS → candles em tempo real, VWAP, agressor acumulado).
- v1.3: Replay engine (consome raw archive e re-emite pra ambientes de teste).
- v2.0: Auth fina por ativo/mercado, multi-tenant.

---

## 15. Glossário Rápido

- **BQT**: Subscribe Book Quote (livro detalhado oferta-a-oferta) — Cedro.
- **SAB**: Subscribe Aggregated Book (livro agregado por preço) — Cedro.
- **SQT**: Subscribe Quote — Cedro.
- **GQT**: Get Quote Trade — Cedro.
- **MQC**: Market Quote Codes (lista de ativos) — Cedro.
- **GPN**: Get Participant Names (lista de corretoras) — Cedro.
- **Aggressor**: lado que executou a ordem (comprador/vendedor).
- **Ticker**: código do ativo (ex: PETR4, WDOZ25).
- **Continuous aggregate**: materialized view incremental do TimescaleDB.
- **WAL**: Write-Ahead Log — garantia de durabilidade antes do flush.
