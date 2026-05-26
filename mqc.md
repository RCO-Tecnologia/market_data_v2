# Cedro Crystal Socket API — Adendo: Comandos `MQC` e `GPN`

> **Propósito deste documento**: Especificação dos comandos `MQC` (lista de ativos por mercado) e `GPN` (lista de corretoras), que não constam da especificação oficial do Cedro Crystal mas são parte do protocolo em produção. Esta documentação foi reconstruída a partir de uso real e segue o mesmo padrão da documentação principal, para ser consumida por uma IA implementadora.
>
> **Pré-requisitos**: leitura prévia da documentação principal (`cedro-crystal-socket-api.md`), em especial das seções 1 (arquitetura), 2 (handshake) e 3 (formato geral das mensagens).

---

## 1. `MQC` — Market Quote Codes (lista de ativos por mercado)

### 1.1 Propósito

Recupera a lista de **códigos de ativos negociados em um determinado mercado**, opcionalmente filtrada por tipo de ativo e/ou subtipo. É o comando usado para **descobrir o universo de ativos** antes de assiná-los via `SQT`, `BQT`, `GQT` etc.

Diferente dos comandos de cotação (`SQT`, `BQT`, …) que são **streaming**, o `MQC` é **request/response**: o servidor responde com a lista completa, marca o fim, e termina. Não há atualizações posteriores.

### 1.2 Sintaxe

Há duas formas principais observadas:

```
MQC <mercado> <tipo_ativo>
MQC <mercado> T <tipo_ativo> S <subtipo>
```

**Parâmetros:**

| Parâmetro | Significado | Tipo |
|-----------|-------------|------|
| `<mercado>` | Nome do mercado. Valores observados: `Bovespa`, `BMF`. | String (case-insensitive na entrada) |
| `<tipo_ativo>` | Código do tipo de ativo. Corresponde ao mesmo enum do índice 45 do `SQT` (ver seção 4.1.3 da documentação principal). Ex: `1`=Ativo à vista, `2`=Opção, `7`=Futuro, `10`=Fracionário, `13`=ETF, `20`=Corp. | Inteiro |
| `T <tipo_ativo>` | Forma estendida do filtro por tipo (usada em conjunto com `S`). | — |
| `S <subtipo>` | Subtipo / vencimento / classe específica dentro do tipo. Para futuros (`T 7`), valores observados: `130`, `60`, `50`, `40`, `30`, `20` (referem-se a códigos internos de séries / produtos). Para opções e à vista (`T 2`), exemplo observado: `150`. | Inteiro |

### 1.3 Forma simples vs. forma estendida

| Forma | Quando usar | Observações |
|-------|-------------|-------------|
| `MQC Bovespa 1` | Listar todos os ativos à vista da Bovespa | Forma direta, sem filtro adicional. |
| `MQC BMF T 7 S 130` | Listar futuros (`T 7`) da BMF com subtipo `130` | Forma necessária quando o mercado tem muitos ativos por tipo e é preciso particionar a consulta por subtipo. |

> **Por que existem duas formas?** Mercados como BM&F têm um volume de futuros e opções que pode estourar limites internos do servidor se solicitados de uma só vez. Particionar por `S` evita timeouts e listas truncadas. Para mercados/tipos pequenos (à vista da Bovespa), a forma simples basta.

### 1.4 Formato da resposta

**Cabeçalho funcional:** `C:<MERCADO>:`

Cada item da lista vem em uma linha própria no formato:

```
C:<MERCADO>:<CÓDIGO_DO_ATIVO>[:campos_adicionais]
```

E a lista é finalizada por:

```
C:<MERCADO>:E
```

**Observações importantes sobre o formato:**

- O `<MERCADO>` na resposta vem em **maiúsculas**, independentemente da capitalização usada no comando. Mercados observados na resposta: `BOVESPA`, `BMF`.
- O `<CÓDIGO_DO_ATIVO>` é o terceiro campo da linha (índice 2 após split por `:`).
- A linha de terminação `C:<MERCADO>:E` tem **exatamente 3 campos** após split — o terceiro é literalmente a letra `E`. Cuidado para não confundir com um código de ativo chamado "E".
- O servidor pode intercalar a resposta do `MQC` com outras mensagens (cotações, livros, etc.) se houver subscrições ativas simultâneas. Linhas que não começam com `C:<MERCADO>:` devem ser **ignoradas (ou roteadas ao parser normal)** durante a coleta da lista.

### 1.5 Exemplo

**Comando:**
```
MQC Bovespa 1
```

**Resposta (exemplo simplificado):**
```
C:BOVESPA:PETR4
C:BOVESPA:VALE3
C:BOVESPA:ITUB4
C:BOVESPA:BBDC4
...
C:BOVESPA:E
```

**Comando com filtro de subtipo:**
```
MQC BMF T 7 S 130
```

**Resposta:**
```
C:BMF:WDOZ25
C:BMF:WINZ25
...
C:BMF:E
```

### 1.6 Comportamento e particularidades

#### 1.6.1 Demora variável por comando

Listas grandes (ex: opções da Bovespa) podem demorar **vários segundos** para chegar completamente. A implementação cliente deve usar timeout generoso por linha — **20 segundos por linha** é um valor que funciona em produção.

#### 1.6.2 Sem confirmação de início

O servidor **não envia uma mensagem de "início da lista"**. A primeira linha já é o primeiro item. Isso significa que se o comando for inválido ou o mercado/tipo não existir, o cliente pode ficar esperando algo que nunca vem — sempre usar timeout.

#### 1.6.3 Linha vazia entre comandos

Aguardar um pequeno intervalo (ordem de dezenas de milissegundos) entre comandos `MQC` consecutivos evita interleaving complicado das respostas. Não é estritamente obrigatório, mas simplifica o parser.

#### 1.6.4 Duplicatas entre subtipos

Diferentes valores de `S` podem retornar ativos repetidos. O cliente deve **deduplicar** após coletar todas as listas.

#### 1.6.5 Tipos e subtipos conhecidos em produção

Combinações conhecidas que funcionam (não exaustivas — descobertas empiricamente):

| Comando | Universo retornado |
|---------|-------------------|
| `MQC Bovespa 1` | Ativos à vista (ações ordinárias e preferenciais) |
| `MQC Bovespa 2` | Opções (sobre ações) |
| `MQC Bovespa 3` | Índices |
| `MQC Bovespa 10` | Fracionário |
| `MQC Bovespa 13` | ETFs |
| `MQC Bovespa 20` | Corp / outros |
| `MQC BMF T 7 S 130` | Futuros — subtipo 130 |
| `MQC BMF T 7 S 60` | Futuros — subtipo 60 |
| `MQC BMF T 7 S 50` | Futuros — subtipo 50 |
| `MQC BMF T 7 S 40` | Futuros — subtipo 40 |
| `MQC BMF T 7 S 30` | Futuros — subtipo 30 |
| `MQC BMF T 7 S 20` | Futuros — subtipo 20 |
| `MQC BMF T 2 S 150` | Opções sobre futuros — subtipo 150 |

Os números `S` correspondem a partições internas do Cedro e não são publicamente documentados; a lista acima foi descoberta empiricamente e cobre o universo prático negociado na B3.

### 1.7 Erros possíveis

Como o `MQC` segue o mesmo modelo de erros do resto do protocolo (seção 10 da documentação principal), os códigos que se aplicam são:

| Código | Quando ocorre |
|--------|---------------|
| `E:1:MQC` | Comando MQC mal-formado ou desativado no servidor. |
| `E:4:MQC` | Faltou parâmetro (ex: `MQC` sem mercado). |
| `E:5:MQC` | Sem parâmetros. |
| `E:10` | Algum parâmetro inválido (ex: tipo de ativo inexistente). |
| `E:17:MQC` | Sem permissão para o serviço de listagem. |

Importante: o protocolo **não retorna erro explícito para "mercado/tipo válido mas vazio"** — nessa situação a resposta é simplesmente `C:<MERCADO>:E` sem nenhum item antes. O cliente deve tratar lista vazia como resultado válido, não como erro.

### 1.8 Estratégia recomendada de implementação

Para descobrir e assinar o universo completo de ativos do dia:

1. **Após o handshake, antes de qualquer subscrição**: enviar todos os comandos `MQC` necessários sequencialmente.
2. Para cada comando, ler linhas até receber `C:<MERCADO>:E` ou estourar timeout.
3. **Cachear** as listas localmente (com TTL de algumas horas ou até o próximo pregão), porque:
   - O universo de ativos muda pouco intradiariamente.
   - Em caso de instabilidade momentânea do servidor de listagem, o cache permite reusar a lista anterior e seguir operando.
4. **Deduplicar** combinando as listas de subtipos.
5. Só **depois de ter o universo completo**, iniciar as subscrições com `SQT`/`BQT`/`GQT`.

### 1.9 Pseudocódigo

```pseudo
function fetchSymbolList(socket, command, lineTimeoutSec=20, maxTimeouts=5):
    socket.write(command + "\n")
    symbols = []
    consecutiveTimeouts = 0

    loop:
        line = socket.readLine(timeout=lineTimeoutSec)

        if line is TIMEOUT:
            consecutiveTimeouts += 1
            if consecutiveTimeouts >= maxTimeouts:
                break
            continue

        if line == EOF:
            break

        trimmed = line.trim()

        // Linha de terminação: "C:<MERCADO>:E"
        parts = trimmed.split(":")
        if parts.length == 3 and parts[0] == "C" and parts[2] == "E":
            break

        // Linha de item: "C:<MERCADO>:<CODIGO>[:...]"
        if parts.length >= 3 and parts[0] == "C" and parts[2] != "":
            symbols.append(parts[2])
            consecutiveTimeouts = 0

        // Outras linhas: ignorar (podem ser de subscrições paralelas)

    return symbols
```

---

## 2. `GPN` — Get Participant Names (lista de corretoras)

### 2.1 Propósito

Recupera a **lista de corretoras (participants)** de um determinado mercado, com seus códigos internos do Cedro. Esses códigos são os mesmos que aparecem nos campos `<corretora>` do `BQT`, `<corretora_comprou>`/`<corretora_vendeu>` do `GQT` e nos índices 60-63 do `SQT`.

Como o `MQC`, o `GPN` é **request/response**, não streaming.

### 2.2 Sintaxe

```
GPN <mercado>
```

**Parâmetros:**

| Parâmetro | Significado | Tipo |
|-----------|-------------|------|
| `<mercado>` | Nome do mercado. Valores observados: `BOVESPA`. | String |

### 2.3 Formato da resposta

**Cabeçalho funcional:** `G:`

Cada corretora vem em uma linha no formato:

```
G:<EXCHANGE>:<CODE>:<NAME>:<CEDRO_ID>:<ACTIVE>
```

**Campos:**

| Campo | Significado | Tipo |
|-------|-------------|------|
| `<EXCHANGE>` | Nome do mercado (ex: `BOVESPA`) | String |
| `<CODE>` | Código oficial da corretora no mercado | String |
| `<NAME>` | Nome/razão social abreviada da corretora | String |
| `<CEDRO_ID>` | Identificador interno do Cedro para a corretora | String/Inteiro |
| `<ACTIVE>` | Indicador de corretora ativa: `1`=ativa, `0`=inativa | String |

### 2.4 Terminação da resposta

**Observação importante**: diferentemente do `MQC` (que termina com `C:<MERCADO>:E`), o `GPN` **não possui uma linha de terminação explícita**. O servidor simplesmente para de enviar linhas `G:...` quando termina a lista.

A implementação cliente deve usar uma das estratégias:

1. **Detectar primeira linha que não começa com `G:`** após já ter recebido pelo menos uma corretora — esse é o sinal de fim.
2. **Timeout curto** (ex: alguns segundos sem nova linha após já ter recebido itens) — assumir que a lista terminou.

A combinação das duas é o mais robusto: parar tanto se vier linha não-`G:` quanto se houver timeout após coleta de pelo menos um item.

### 2.5 Exemplo

**Comando:**
```
GPN BOVESPA
```

**Resposta (exemplo):**
```
G:BOVESPA:3:XP INVESTIMENTOS:3:1
G:BOVESPA:8:CITIBANK:8:1
G:BOVESPA:14:CM CAPITAL:14:1
G:BOVESPA:72:BRADESCO S/A CTVM:72:1
G:BOVESPA:90:EASYNVEST:90:1
G:BOVESPA:120:CLEAR:120:1
G:BOVESPA:131:ITAU CV:131:1
...
```

(A lista termina implicitamente — sem linha de fim explícita.)

### 2.6 Comportamento

#### 2.6.1 Lista grande, mas finita

A lista típica da BOVESPA tem dezenas de corretoras. O download completo costuma ser **rápido** (menos de 1 segundo em condições normais), porque é uma resposta única e curta — diferente do `MQC` que pode trazer milhares de itens.

#### 2.6.2 Códigos persistentes

Os pares `<CODE>` ↔ `<CEDRO_ID>` ↔ `<NAME>` são razoavelmente estáveis ao longo do tempo. Mudam principalmente quando há fusões, mudanças de razão social ou novas corretoras entrando. Vale fazer cache.

#### 2.6.3 Quando o servidor não responde

Se o servidor não retornar nenhuma linha `G:` dentro de alguns segundos, é provável que:

- O usuário não tem permissão (deveria vir um `E:17:GPN` ou `E:3:GPN`, mas pode silenciar).
- O mercado solicitado não existe ou foi escrito incorretamente.
- O serviço está indisponível.

A implementação deve ter um limite de tentativas e prosseguir sem a lista de corretoras se necessário (sacrifício: não conseguirá nomear corretoras nos dados de book/trades).

### 2.7 Erros possíveis

| Código | Quando ocorre |
|--------|---------------|
| `E:1:GPN` | Comando GPN mal-formado. |
| `E:3:GPN:<mercado>` | Sem permissão para o mercado. |
| `E:4:GPN` | Parâmetro vazio. |
| `E:10` | Parâmetro inválido (mercado não reconhecido). |
| `E:17:GPN` | Sem permissão para o serviço. |

### 2.8 Estratégia recomendada de implementação

1. **Executar `GPN <mercado>` uma vez após o handshake**, antes das subscrições de cotação.
2. Coletar as linhas `G:...` até:
   - Receber uma linha que não comece com `G:` (após já ter pelo menos uma corretora), OU
   - Esgotar o timeout sem novas linhas.
3. Construir um dicionário `cedro_id → {code, name, exchange, active}` para enriquecer os dados de book e trades em tempo real.
4. **Persistir o dicionário** (cache local) para uso entre reinícios e para tolerância a falhas do `GPN`.

### 2.9 Pseudocódigo

```pseudo
function fetchBrokers(socket, market, lineTimeoutSec=10, maxTimeouts=5):
    socket.write("GPN " + market + "\n")
    brokers = []
    consecutiveTimeouts = 0

    loop:
        line = socket.readLine(timeout=lineTimeoutSec)

        if line is TIMEOUT:
            consecutiveTimeouts += 1
            if consecutiveTimeouts >= maxTimeouts:
                break
            // Se já temos corretoras, assumir que a lista terminou
            if brokers.length > 0:
                break
            continue

        if line == EOF:
            break

        trimmed = line.trim()

        if not trimmed.startsWith("G:"):
            // Primeira linha não-G: depois de já ter coletado: lista terminou
            if brokers.length > 0:
                break
            // Antes de ter coletado nada: pode ser ruído, continuar
            continue

        parts = trimmed.split(":")
        if parts.length >= 6:
            brokers.append({
                exchange: parts[1],
                code:     parts[2],
                name:     parts[3],
                cedroId:  parts[4],
                active:   parts[5] == "1"
            })
            consecutiveTimeouts = 0

    return brokers
```

---

## 3. Resumo (quick reference)

| Comando | Categoria | Streaming? | Tem terminador? | Cancelar com |
|---------|-----------|------------|-----------------|--------------|
| `MQC <mercado> <tipo>` | Descoberta de universo | Não (request/response) | Sim: `C:<MERCADO>:E` | — |
| `MQC <mercado> T <tipo> S <subtipo>` | Descoberta de universo (filtrada) | Não | Sim: `C:<MERCADO>:E` | — |
| `GPN <mercado>` | Lista de corretoras | Não | **Não** (termina implicitamente) | — |

---

## 4. Como esses comandos se encaixam no fluxo de bootstrap

A ordem recomendada de comandos imediatamente após o handshake (`You are connected`):

1. **`GPN BOVESPA`** — obter a lista de corretoras (rápido, < 1s).
2. **`MQC ...`** (todos os comandos relevantes ao escopo desejado) — descobrir o universo de ativos.
3. **`NEM A <agência>`** para cada agência de interesse — assinar notícias.
4. **`GQT <ativo> N ...`** em lotes — solicitar snapshot histórico de trades para os ativos relevantes.
5. **`SQT <ativo>` / `GQT <ativo> S` / `BQT <ativo>`** em lotes — ativar as subscrições de streaming.

Etapas 1-3 são quase instantâneas. Etapa 2 pode levar dezenas de segundos no total se for solicitar todo o universo da B3. Etapas 4-5 devem ser **lotadas** (várias subscrições por `write`) para evitar gargalo de rede.

---

## 5. Checklist de implementação dos comandos deste adendo

- [ ] Função `fetchSymbolList(market, type, [subtype])` que executa `MQC` e coleta até `C:<MERCADO>:E`.
- [ ] Tolerância a linhas intercaladas (cotações, etc.) que possam aparecer durante a coleta.
- [ ] Timeout por linha (sugerido: 20s) e contador de timeouts consecutivos (sugerido: máx 5).
- [ ] Cache local da lista de ativos com TTL apropriado.
- [ ] Função `fetchBrokers(market)` que executa `GPN` e coleta até primeira linha não-`G:` ou timeout.
- [ ] Cache local do dicionário de corretoras `cedro_id → {code, name, exchange, active}`.
- [ ] Deduplicação entre múltiplos comandos `MQC` com subtipos diferentes.
- [ ] Bootstrap orquestrado: `GPN` → `MQC*` → subscrições.
- [ ] Tratamento de erros `E:1`, `E:3`, `E:4`, `E:10`, `E:17` específicos desses comandos.
- [ ] Logging do número de itens recebidos por comando, para monitorar saúde da listagem.