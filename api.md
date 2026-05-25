# Cedro Crystal Socket API — Documentação Técnica para Implementação

> **Propósito deste documento**: Especificação completa do protocolo de socket do Cedro Crystal, estruturada para ser consumida por uma IA que irá implementar um cliente desta API. Contém todos os comandos, formatos de mensagem, índices, códigos, cenários de erro e regras arquiteturais necessárias para uma implementação correta e robusta.
>
> **Fonte**: Especificação oficial Cedro Technologies — Projeto Cedro Crystal, Módulo SERVER (criado 18/11/2005, última revisão 08/05/2025).

---

## 1. Visão Arquitetural (LEIA PRIMEIRO)

### 1.1 Modelo de comunicação

- **Protocolo de transporte**: TCP (via Telnet) na **porta 81**.
- **Modelo**: **Streaming server**, NÃO request/response.
- **Codificação**: ASCII / texto puro, mensagens delimitadas por linhas.

### 1.2 Regras arquiteturais CRÍTICAS (não-negociáveis)

Estas regras devem moldar o design da implementação desde o início:

1. **NÃO simular request/response.** O cliente não pode ficar pedindo informações repetidamente. O modelo correto é: **assinar uma vez** (`SQT`, `BQT`, etc.) e **consumir o stream contínuo** que o servidor envia conforme há alterações.

2. **Arquitetura obrigatória de duas threads (mínimo)**:
   - **Thread A (reader)**: responsabilidade ÚNICA de drenar bytes do socket TCP e enfileirar mensagens brutas em uma fila interna. Não faz parsing, não faz lógica de negócio.
   - **Thread B (processor)**: consome a fila interna e faz parsing/processamento das mensagens.
   - **Motivo**: se o cliente não drenar o socket rápido o suficiente, o buffer TCP enche, o servidor detecta isso e **desconecta o cliente**.

3. **Idempotência de subscrições**: o servidor mantém uma "lista de monitoramento" por usuário. Reassinar um ativo já assinado não duplica mensagens, mas o comando `<comando> <ativo> N` permite obter apenas um snapshot sem entrar na lista de monitoramento.

4. **Conexão única por usuário por servidor**: abrir uma segunda conexão com mesmo usuário derruba a primeira (ver `E:6` e `E:8`).

---

## 2. Handshake de Conexão

### 2.1 Sequência exata

Após abrir o socket TCP na porta 81:

```
Servidor → "Welcome to Cedro Crystal"
Servidor → "Username:" (após receber Software Key)
Cliente  → <Software Key>\r\n    # Se não houver, enviar linha vazia (apenas \r\n)
Cliente  → <Username>\r\n
Cliente  → <Password>\r\n
Servidor → "You are connected"
```

### 2.2 Pseudocódigo de referência

```pseudo
socket = tcp.connect(host, 81)
socket.readUntil("Welcome to Cedro Crystal")
socket.write(softwareKey + "\r\n")   // string vazia se não houver
socket.readUntil("Username:")
socket.write(username + "\r\n")
socket.readUntil("Password:")
socket.write(password + "\r\n")
socket.readUntil("You are connected")
// A partir daqui, conexão pronta para comandos.
```

### 2.3 Observações de robustez

- Implementar **timeout** em cada `readUntil` (sugestão: 10s).
- Capturar mensagens de erro de autenticação (provavelmente uma das `E:*` definidas na seção 8).
- Considerar que o servidor pode enviar prompts em ordem ligeiramente variável; preferir parsing por padrão (regex `Username:`, `Password:`) ao invés de posição fixa.

---

## 3. Formato Geral das Mensagens

### 3.1 Estrutura

Toda mensagem do servidor segue o padrão:

```
<CABEÇALHO_FUNCIONAL>:<CORPO>!
```

- **Cabeçalho funcional**: identifica o tipo de resposta (ex: `T` para quote, `B` para book, `V` para trades, `Z` para aggregated book, `O` para news, `VAP`, `GTC`, `GNA`).
- **Corpo**: pares `<chave>:<valor>` separados por `:`.
- **Terminador**: caractere `!` (apenas para mensagens do comando `SQT`; outros comandos terminam por `\n`).
- **Separador de campos**: `:`.

### 3.2 Cabeçalhos por comando

| Comando enviado | Cabeçalho da resposta |
|-----------------|----------------------|
| `SQT`           | `T:<ativo>:<hora>`   |
| `BQT`           | `B:<ativo>:`         |
| `SAB`           | `Z:<ativo>:`         |
| `GQT`           | `V:<ativo>:`         |
| `NEM`           | `O:`                 |
| `GNA`           | `GNA:`               |
| `VAP`           | `VAP:`               |
| `GTC`           | `GTC:`               |
| Erros           | `E:<código>:...`     |

### 3.3 Parsing recomendado

Tokenizar por `:` mas **atenção a campos que podem conter `:`** (raro, mas possível em strings de descrição). Uma estratégia robusta:

1. Identificar o cabeçalho (primeiros 1-3 tokens).
2. Para `SQT`, o corpo é uma sequência de pares `índice:valor` — fazer parsing em pares.
3. Para os demais, usar split posicional conforme a definição de cada comando.

---

## 4. Comandos de Cotação

### 4.1 `SQT` — Subscribe Quote

Assina cotação de um ativo (streaming contínuo de atualizações).

**Sintaxe:**
```
SQT <ativo>          # assina (entra na lista de monitoramento)
SQT <ativo> N        # apenas snapshot (não monitora)
```

**Cabeçalho da resposta:** `T:<ativo>:<hora>`

**Corpo:** sequência de pares `:<índice>:<valor>` terminada por `!`.

**Exemplo de resposta:**
```
T:PETR4:101758:1:20070926:2:59.95:3:59.93:4:59.96:5:101757:6:0:7:800:8:361:9:354600!
T:PETR4:155613:3:43.01:19:2000:60:239:17:4000!
```

A primeira mensagem traz o snapshot completo; mensagens subsequentes trazem apenas os índices alterados.

#### 4.1.1 Tabela COMPLETA de índices SQT

| Índice | Significado | Tipo |
|--------|-------------|------|
| 0  | Horário da última modificação | HHMMSS |
| 1  | Data da última modificação | YYYYMMDD |
| 2  | Preço do último negócio | Float |
| 3  | Melhor oferta de compra | Float |
| 4  | Melhor oferta de venda | Float |
| 5  | Horário do último negócio | HHMMSS |
| 6  | Quantidade do negócio atual | Inteiro |
| 7  | Quantidade do último negócio | Inteiro |
| 8  | Quantidade de negócios realizados | Inteiro |
| 9  | Volume acumulado dos negócios | Inteiro |
| 10 | Volume financeiro dos negócios | Float |
| 11 | Maior preço do dia | Float |
| 12 | Menor preço do dia | Float |
| 13 | Preço de fechamento do dia anterior | Float |
| 14 | Preço de abertura | Float |
| 15 | Horário da melhor oferta de compra | HHMMSS |
| 16 | Horário da melhor oferta de venda | HHMMSS |
| 17 | Volume acumulado das melhores ofertas de compra | Float |
| 18 | Volume acumulado das melhores ofertas de venda | Float |
| 19 | Volume da melhor oferta de compra | Inteiro |
| 20 | Volume da melhor oferta de venda | Inteiro |
| 21 | Variação | Float |
| 36 | Preço de fechamento da última semana | Float |
| 37 | Preço de fechamento do último mês | Float |
| 38 | Preço de fechamento do último ano | Float |
| 39 | Preço de abertura do dia anterior | Float |
| 40 | Maior preço do dia anterior | Float |
| 41 | Menor preço do dia anterior | Float |
| 42 | Média | Float |
| 43 | VHDaily | Float |
| 44 | Código do Mercado (enum — ver 4.1.2) | Inteiro |
| 45 | Código do tipo do ativo (enum — ver 4.1.3) | Inteiro |
| 46 | Lote padrão | Inteiro |
| 47 | Descrição do ativo (ex: "DOL", "INDICE BOVESPA") | String(10) |
| 48 | Nome de classificação | String |
| 49 | Forma de cotação | Inteiro |
| 50 | Intraday Date (FORCES) | YYYYMMDDHHMMSS |
| 51 | LastTrade Date (FORCES) | YYYYMMDDHHMMSS |
| 52 | Descrição abreviada do ativo | String |
| 53 | Identificador do negócio cancelado | String |
| 54 | Data do último negócio | YYYYMMDD |
| 56 | Sentido das ofertas não atendidas ao preço de abertura | Char: `A`=Compra, `V`=Venda, `0`=Não informado |
| 57 | Quantidade não atendida ao preço de abertura | Inteiro |
| 58 | Horário programado para abertura do papel | HHMMSS |
| 59 | Horário reprogramado para abertura do papel | HHMMSS |
| 60 | Código da corretora — melhor oferta de compra | Inteiro |
| 61 | Código da corretora — melhor oferta de venda | Inteiro |
| 62 | Código da corretora — última compra | Inteiro |
| 63 | Código da corretora — última venda | Inteiro |
| 64 | Data do vencimento (opções) | YYYYMMDD |
| 65 | Expirado | Inteiro |
| 66 | Número total de papéis | String |
| 67 | Status do instrumento (enum — ver 4.1.4) | Inteiro |
| 72 | Tipo da opção: `A`=Americana, `E`=Europeia, `0`=não existe | Char |
| 74 | Direção da opção: `P`=venda, `C`=compra | Char |
| 81 | Símbolo do ativo pai (para opções) | String |
| 82 | Preço teórico de abertura | Float |
| 83 | Quantidade teórica | Inteiro |
| 84 | Status do ativo (enum — ver 4.1.5) | Inteiro |
| 85 | Preço de Exercício | Float |
| 86 | Diff (Preço Atual − Previous) | Float |
| 87 | Data do Previous | YYYYMMDD |
| 88 | Fase do grupo do ativo (enum — ver 4.1.6) | String |
| 89 | Média do dia anterior | Float |
| 90 | Intervalo de Margem (mercado BTC) | Float |
| 94 | Volume médio nos últimos 20 dias | Float |
| 95 | Market Capitalization | Float |
| 96 | Tipo de Mercado: `RT`=RealTime, `D`=Delay, `EOD`=End of Day | String |
| 97 | Preço de fechamento em uma semana | Float |
| 98 | Preço de fechamento em um mês | Float |
| 99 | Preço de fechamento em um ano | Float |
| 100 | Quantidade de contratos abertos | Inteiro |
| 101 | Número dias úteis até o vencimento | Inteiro |
| 102 | Número dias para o vencimento | Inteiro |
| 103 | Ajuste do dia | Float |
| 104 | Ajuste do dia anterior | Float |
| 105 | SecurityId (BMF FIX) | String |
| 106 | TickDirection (BMF FIX): `+`, `0+`, `-`, `0-` | String(2) |
| 107 | TunnelUpperLimit | Float |
| 108 | TunnelLowerLimit | Float |
| 109 | TradingPhase (BMF FIX) | String(2) |
| 110 | TickSize | Inteiro |
| 111 | Volume mínimo de negociação do instrumento | Inteiro |
| 112 | Intervalo mínimo para incrementos de preço | Float |
| 113 | Quantidade mínima para o instrumento em uma oferta | Inteiro |
| 114 | Quantidade máxima para o instrumento em uma oferta | Inteiro |
| 115 | Número único de identificação do instrumento | Inteiro |
| 116 | Moeda utilizada no preço (`BRL`, `EUR`, `USD`) | String |
| 117 | SecurityType (`FUT`, `OPT`, `SPOT`, `SOPT`, `FOPT`, `DTERM`) | String(32) |
| 118 | Código de negociação (Security Sub Type) — ex: `1003`=ON, `1107`=Non tradable Index, `1124`=Fixed Income ETF | Inteiro |
| 119 | Produto associado ao instrumento | Inteiro |
| 120 | Mês e ano de vencimento | YYYYMM |
| 121 | Preço de exercício da opção | Float |
| 122 | Moeda do preço de exercício (`BRL`, `EUR`, `USD`) | String |
| 123 | Multiplicador do contrato | Float |
| 124 | Código que representa o tipo de preço do instrumento | Inteiro |
| 125 | Horário em que o instrumento não é mais negociável | YYYYMMDDHHMMSS |
| 126 | Indica o grupo ao qual o ativo pertence | String(15) |
| 127 | Ajuste atual em taxa | Float |
| 128 | Ajuste anterior em taxa | Float |
| 129 | Data do Ajuste atual em taxa | YYYYMMDD |
| 130 | Número de saques até data de vencimento | Inteiro |
| 134 | Variação do volume da hora vs. média dos últimos 20 dias | Float |
| 135 | Variação do volume até a hora vs. média dos últimos 20 dias | Float |
| 136 | Código do setor do Ativo | Inteiro |
| 137 | Código do subsetor do Ativo | Inteiro |
| 138 | Código do segmento do Ativo (`-1` se não existir) | Inteiro |
| 139 | Tipo do ajuste atual em taxa | String |
| 140 | Preço de referência | Double |
| 141 | Data do preço de referência | String |
| 142 | Horário da última modificação (com milissegundos) | HHMMSSmmm |
| 143 | Horário do último negócio (com milissegundos) | HHMMSSmmm |
| 144 | Horário da melhor oferta de compra (com milissegundos) | HHMMSSmmm |
| 145 | Horário da melhor oferta de venda (com milissegundos) | HHMMSSmmm |
| 146 | Variação usando ajuste do dia anterior | Float |
| 147 | Diff (Preço Atual − Ajuste do dia anterior) | Float |
| 148 | Tunnel Upper Auction Limit | Double |
| 149 | Tunnel Lower Auction Limit | Double |
| 150 | Tunnel Upper Rejection Limit | Double |
| 151 | Tunnel Lower Rejection Limit | Double |
| 152 | Tunnel Upper Static Limit | Double |
| 153 | Tunnel Lower Static Limit | Double |
| 154 | Data em que o ativo estará expirado | YYYYMMDD |
| 155 | Mínima da semana | Double |
| 156 | Máxima da semana | Double |
| 157 | Mínima do mês | Double |
| 158 | Máxima do mês | Double |
| 159 | Mínima do ano | Double |
| 160 | Máxima do ano | Double |

**Campos EXCLUSIVOS do mercado Tesouro Direto:**

| Índice | Significado | Tipo |
|--------|-------------|------|
| 200 | Preço unitário | Float |
| 201 | Valor da taxa (Rentabilidade) | Float |
| 202 | Valor mínimo de aplicação | Float |
| 203 | Mercado | Inteiro |
| 204 | Código do título | String |
| 205 | Código do tipo | Inteiro |
| 206 | Nome do tipo | String |
| 207 | Selic | String |
| 208 | Data de emissão | YYYYMMDD |
| 209 | Negócio | Inteiro |
| 210 | Valor base | Float |
| 211 | Valor da taxa de compra | Float |
| 212 | Valor da taxa de venda | Float |
| 213 | Código indexador | Inteiro |
| 214 | Nome indexador | String |
| 215 | Nome do título | String |

#### 4.1.2 Enum — Código do Mercado (índice 44)

| Código | Mercado |
|--------|---------|
| 1 | Bovespa |
| 2 | Dow Jones |
| 3 | BM&F |
| 4 | Índices |
| 5 | Money |
| 7 | Forex |
| 8 | Indicators |
| 10 | NYSE |
| 12 | Nasdaq |
| 13 | CFD |
| 30 | Bitcoin |
| 44 | Datagro |
| 45 | INews |
| 52 | Amex |
| 64 | Tesouro Direto |
| 76 | NYSE FMV |
| 77 | Nasdaq FMV |
| 78 | Amex FMV |

#### 4.1.3 Enum — Tipo do Ativo (índice 45)

| Código | Tipo |
|--------|------|
| 1 | Ativo à vista |
| 2 | Opção |
| 3 | Índice |
| 4 | Commodity |
| 5 | Moeda |
| 6 | Termo |
| 7 | Futuro |
| 8 | Leilão |
| 9 | Bônus |
| 10 | Fracionário |
| 11 | Exercício de opção |
| 12 | Indicador |
| 13 | ETF |
| 15 | Volume |
| 16 | Opção sobre a vista |
| 17 | Opção sobre futuro |
| 18 | Ativo de teste |
| 19 | Estratégia |
| 20 | Corp |
| 21 | SECLOAN (Aluguel BTB) |
| 22 | Tesouro Direto |

#### 4.1.4 Enum — Status do Instrumento (índice 67)

| Código | Status |
|--------|--------|
| 101 | Normal |
| 102 | Leilão |
| 105 | Suspenso |
| 118 | Congelado |
| -1 | Vazio (apenas internacionais) |

#### 4.1.5 Enum — Status do Ativo (índice 84)

| Código | Status |
|--------|--------|
| 0 | Normal |
| 1 | Congelado |
| 2 | Suspenso |
| 3 | Leilão |
| 4 | Inibido |

#### 4.1.6 Enum — Fase do Grupo do Ativo (índice 88)

| Código | Fase |
|--------|------|
| `P` | Pré-abertura |
| `A` | Abertura (sessão normal) |
| `PN` | Pré-fechamento |
| `N` | Fechamento |
| `E` | Pré-abertura do after |
| `R` | Abertura After |
| `NE` | Fechamento do after |
| `F` | Final |
| `NO` | Fechado |
| `T` | Pausado |

### 4.2 `USQ` — Unsubscribe Quote

**Sintaxe:** `USQ <ativo>`

**Exemplo:** `USQ petr4`

Cancela a assinatura de cotação. Não retorna confirmação explícita; apenas para o fluxo de mensagens `T:<ativo>:...`.

---

## 5. Comandos de Livro de Ofertas

### 5.1 `BQT` — Subscribe Book Quote (livro completo, oferta a oferta)

Assina o livro de ofertas detalhado de um ativo.

**Sintaxe:** `BQT <ativo>`

**Cabeçalho da resposta:** `B:<ativo>:`

**Tipos de mensagem do corpo:**

| Tipo | Formato |
|------|---------|
| Adição (`A`) | `A:<posição>:<direção>:<preço>:<quantidade>:<corretora>:<data/hora>:<OrderID>:<tipo da oferta>` |
| Atualização (`U`) | `U:<posição_nova>:<posição_antiga>:<direção>:<preço>:<quantidade>:<corretora>:<data/hora>:<OrderID>:<tipo da oferta>` |
| Fim das mensagens iniciais (`E`) | `E` |
| Cancelamento (`D`) | `D:<tipo>:<direção>:<posição>` |

**Significado dos campos:**

| Campo | Descrição | Tipo |
|-------|-----------|------|
| `<posição>` | Posição da oferta no livro (0 = topo) | Inteiro |
| `<direção>` | `A`=oferta de compra (Ask/Bid), `V`=oferta de venda | Char |
| `<preço>` | Preço da oferta | Float |
| `<quantidade>` | Quantidade da oferta | Inteiro |
| `<corretora>` | Código da corretora detentora da oferta | Inteiro |
| `<data/hora>` | Data e hora da oferta | DDMMHHMM |
| `<posição_nova>` | Nova posição após atualização | Inteiro |
| `<posição_antiga>` | Posição que ocupava antes | Inteiro |
| `<OrderID>` | Identificador único da ordem (escopo: corretora + instrumento + lado). Pode mudar se a ordem for editada. **Para identificar oferta de forma única e completa**: combinar `OrderID` + corretora + instrumento + direção. | String |
| `<tipo da oferta>` | `L`=Limitada, `O`=Oferta ao preço de Abertura | Char |
| `<tipo>` (em `D`) | `1`=cancela apenas a posição indicada; `2`=cancela todas as melhores que a posição (inclusive ela); `3`=cancela TUDO (compra e venda) — neste caso não vem direção/posição | Inteiro |

**Exemplo:**
```
B:PETR4:A:0:A:99.99:100:131:11041005
B:PETR4:U:1:0:A:99.98:500:37:11041130
B:PETR4:D:1:V:4
B:PETR4:D:2:A:2
B:PETR4:D:3
```

#### 5.1.1 Estratégia de manutenção de estado do livro (BQT)

A implementação deve manter uma estrutura por ativo, algo como:

```pseudo
book[ativo] = {
  compra: ArrayList<Oferta>,   // indexado por posição
  venda:  ArrayList<Oferta>
}
```

Aplicar as mensagens conforme o tipo:

- **`A` (adição)**: inserir oferta na posição indicada do lado indicado, deslocando as posteriores.
- **`U` (atualização)**: remover da `posição_antiga`, inserir em `posição_nova`. Se `posição_antiga == posição_nova`, apenas substituir os campos.
- **`D` tipo 1**: remover apenas aquela posição.
- **`D` tipo 2**: remover da posição 0 até a posição indicada (inclusive).
- **`D` tipo 3**: limpar AMBOS os lados do livro inteiro. Não há `<direção>` nem `<posição>` na mensagem.
- **`E`**: fim do snapshot inicial; daqui em diante são updates incrementais.

### 5.2 `UBQ` — Unsubscribe Book Quote

**Sintaxe:** `UBQ <ativo>`

### 5.3 `SAB` — Subscribe Aggregated Book (livro agregado por preço)

Versão agregada do livro: ofertas no mesmo preço são somadas em uma única linha.

**Sintaxe:** `SAB <ativo> [N]`

- `N` opcional: solicita snapshot único sem monitoramento contínuo.

**Cabeçalho da resposta:** `Z:<ativo>:`

**Tipos de mensagem:**

| Tipo | Formato |
|------|---------|
| Adição (`A`) | `A:<posição>:<direção>:<preço>:<quantidade>:<número_de_ofertas>:<data/hora>` |
| Atualização (`U`) | `U:<posição>:<direção>:<preço>:<quantidade>:<número_de_ofertas>:<data/hora>` |
| Fim (`E`) | `E` |
| Cancelamento (`D`) | `D:<tipo>:<direção>:<posição>` |

**Campos extras vs. BQT:**

- `<número_de_ofertas>` (Inteiro): quantidade de ofertas individuais agregadas naquela linha de preço.
- `<tipo>` em `D` só aceita: `1`=cancela posição específica; `3`=cancela tudo (sem direção/posição).

**Exemplo:**
```
Z:PETR4:D:3
Z:PETR4:A:0:A:32.580:200:1:08040214
Z:PETR4:A:1:A:32.550:100:1:08040214
Z:PETR4:A:2:A:32.530:100:1:08040214
Z:PETR4:A:3:A:32.500:1000:3:08040214
Z:PETR4:A:4:A:32.310:2000:1:08040214
Z:PETR4:A:0:V:32.600:3200:9:08040214
Z:PETR4:A:1:V:32.620:600:1:08040214
Z:PETR4:A:2:V:32.640:300:1:08040214
Z:PETR4:A:3:V:32.650:1700:4:08040214
Z:PETR4:A:4:V:32.660:400:1:08040214
Z:PETR4:E
```

### 5.4 `UAB` — Unsubscribe Aggregated Book

**Sintaxe:** `UAB <ativo>`

---

## 6. Comandos de Negócios (Trades)

### 6.1 `GQT` — Get Quote Trade

Solicita os negócios realizados no dia para um ativo. Tem **dois modos distintos**.

#### 6.1.1 Modo Subscribe (streaming contínuo)

**Sintaxe:**
```
GQT <ativo> S [<quantidade_negócios>] [<identificador_do_negócio>] [<ASC|DESC>] [<C>]
```

**Regras de parâmetros opcionais (importantes!):**

- Para usar `<identificador_do_negócio>`, é **obrigatório** especificar `<quantidade_negócios>`.
- Para usar `<ASC|DESC>` ou `<C>`, é **obrigatório** especificar `<identificador_do_negócio>`.
- O `<identificador_do_negócio>` funciona como operador **`>` (maior que)**: retorna apenas negócios com ID posterior.
  - Ex.: enviar `0` → retorna negócios 10, 20, 30, ...
  - Ex.: enviar `10` → retorna negócios 20, 30, ...
- `<C>` indica que o snapshot inicial será retornado **compactado**.

#### 6.1.2 Modo Snapshot (apenas histórico, sem streaming)

**Sintaxe:**
```
GQT <ativo> N <quantidade_negócios> <offset> <identificador_requisição> [<ASC|DESC>] [<C>]
```

**Exemplos válidos:**
```
GQT PETR4 N 2 50 XXX
GQT PETR4 N 2 50 XXX DESC C
GQT PETR4 N 2 50 XXX C
GQT PETR4 S
GQT PETR4 S 10
GQT DI1F11 S 10 2020
GQT PETR4 S 10 10 DESC
GQT PETR4 S 10 10 DESC C
GQT PETR4 S 10 10 C
```

#### 6.1.3 Formato das mensagens

**Cabeçalho funcional:** `V:<ativo>:`

| Tipo | Formato |
|------|---------|
| Negócio (subscribe) | `<operação>:<horário>:<preço>:<corretora_comprou>:<corretora_vendeu>:<quantidade>:<id_negócio>:<condição_trade>:<agressor>:<condição_trade_original>` |
| Negócio (snapshot) | `<operação>:<horário>:<preço>:<corretora_comprou>:<corretora_vendeu>:<quantidade>:<id_negócio>:<id_requisição>:<condição_trade>:<agressor>:<condição_trade_original>` |
| Remoção de negócio | `<operação>:<id_negócio>` |
| Remoção de todos | `<operação>` |
| Fim (subscribe) | `E` |
| Fim (snapshot) | `E:<id_requisição>` |

#### 6.1.4 Enum `<operação>`

| Código | Significado |
|--------|-------------|
| `A` | Adição de negócio |
| `D` | Remoção de negócio |
| `R` | Remoção de todos os negócios |

#### 6.1.5 Enum `<condição_trade>` (inteiro)

| Código | Condição |
|--------|----------|
| 0 | Não Direto |
| 1 | Direto |
| 2 | RLP |
| 3 | RFQ |
| 4 | MIDPOINT TRADE |
| 5 | OPENING PRICE (leilão) |
| 6 | POINT IN TIME AUCTION |

#### 6.1.6 Enum `<agressor>`

| Código | Significado |
|--------|-------------|
| `I` | Indefinido |
| `A` | Comprador |
| `V` | Vendedor |

#### 6.1.7 Enum `<condição_trade_original>`

**IMPORTANTE**: é uma **lista delimitada por ESPAÇOS** (não `:`).

| Código | Condição |
|--------|----------|
| `0` | Default (No condition) |
| `R` | Opening Price |
| `X` | Crossed |
| `L` | Last Trade at the Same Price Indicator |
| `P` | Imbalance more buyers |
| `Q` | Imbalance more sellers |
| `U` | Exchange Last |
| `3` | Multi Asset Trade (Termo Vista) |
| `1` | Leg trade |
| `2` | Marketplace entered trade (trade on behalf) |
| `IM` | Implied |
| `PT` | Block Book Trade |
| `RF` | Equities: RFQ Trade |
| `RL` | RLP Trade |
| `MP` | Midpoint Trade |
| `TC` | Trade at Close |
| `TA` | Trade at Average |
| `SW` | Sweep |

### 6.2 `UQT` — Unsubscribe Quote Trade

**Sintaxe:** `UQT <ativo>`

---

## 7. Comandos de Notícias

### 7.1 `GNA` — Get News Agency

Lista todas as agências de notícias disponíveis.

**Sintaxe:** `GNA`

**Cabeçalho:** `GNA:`

**Formato dos itens:**
```
GNA:<símbolo>:<tipo>:<código>:<descrição>:<tipo_agência>:<cor>
```

Terminado por uma linha `END`.

**Campos:**

| Campo | Significado |
|-------|-------------|
| `<símbolo>` | Símbolo da agência |
| `<tipo>` | `C`=privada, `O`=pública |
| `<código>` | Código numérico da agência |
| `<descrição>` | Descrição textual |
| `<tipo_agência>` | `1`=Notícias, `2`=Análise |
| `<cor>` | Código da cor da agência |

**Exemplo:**
```
GNA:BMF:O:4:BMF News
GNA:BOV:O:3:Bovespa News
GNA:BRNEW:O:7:Agência Brasil
GNA:CAPI:O:19:CMCapital News
```

> **Nota**: nos exemplos do PDF os campos `<tipo_agência>` e `<cor>` não aparecem em todas as linhas — a implementação deve tolerar formato com campos opcionais ao final.

### 7.2 `NEM` — News

Três modos de operação:

#### 7.2.1 Subscribe (streaming de novas notícias)
```
NEM A <agência>
```
Resposta (cabeçalho `O:`):
```
A:<agência>:<código>:<data>:<horário>:<categoria>:<tamanho_título>:<título>
```

#### 7.2.2 Últimas notícias (histórico)
```
NEM L <id_requisição> <quantidade> <agência> <palavra_chave>
```
Resposta:
```
L:<id_requisição>:<agência>:<código>:<data>:<horário>:<categoria>:<tamanho_título>:<título>
```
Terminado por: `L:<id_requisição>:END`

#### 7.2.3 Corpo de uma notícia
```
NEM N <id_requisição> <agência> <código>
```
Resposta:
```
N:<id_requisição>:<agência>:<código>:<corpo>
```

**Observação CRÍTICA sobre o corpo**: as quebras de linha (`\r\n` = caracteres ASCII 013 010) são **substituídas pelo caractere ASCII 003 (ETX)**. A implementação deve converter o ETX de volta para `\n` ao apresentar o texto.

Se o código da notícia não existir, retorna `E:16:<id_requisição>` (ver seção 8).

**Exemplos:**
```
> NEM a bov
O:A:BOV:1565281:20090617:134432:1:46:LEILAO DE IBOVT40 (OPV IBOV AGO/40.000) ATE 13

> NEM l 123 10
O:L:123:BOV:1565287:20090617:134715:1:62:17:06-OFERTAS DISPONIVEIS NO BANCO DE TITULOS CBLC-BTC-4 13:46
...
O:L:123:END

> NEM n 123 cfn 2221941
O:N:123:CFN:2221941:Mineradores Mamani e Peña recebem alta do hospital no Chile
```

### 7.3 `UNE` — Unsubscribe News

**Sintaxe:** `UNE <agência>`

---

## 8. Volume at Price (VAP)

### 8.1 `VAP` — Volume at Price

Apontamento de volume negociado por preço.

**Sintaxe:**
```
VAP <ativo> [<período>]
VAP <ativo> <h> <data_início> [<data_fim>] [<acc>]
```

Onde `<período>` é em **minutos** (ex: 5 = últimos 5 minutos).

**Cabeçalho:** `VAP:`

**Formato dos itens:**
```
<ativo>:<preço_negociado>:<qtd_neg_comprador>:<volume_comprador>:<qtd_neg_vendedor>:<volume_vendedor>:<qtd_neg_direto>:<volume_direto>:<qtd_neg_indefinido>:<volume_indefinido>:<período>:<qtd_neg_RLP>:<volume_RLP>:<qtd_neg_leilão>:<volume_leilão>
```

**Terminadores:**
- Sem período: `<ativo>:E`
- Com período: `<ativo>:E:<período>`

**Campos:**

| Campo | Significado | Tipo |
|-------|-------------|------|
| `<ativo>` | Nome do ativo | String |
| `<preço_negociado>` | Preço em que foi negociado | Float |
| `<qtd_neg_comprador>` | Negócios com agressor = comprador | Float |
| `<volume_comprador>` | Volume agressor = comprador | Float |
| `<qtd_neg_vendedor>` | Negócios com agressor = vendedor | Float |
| `<volume_vendedor>` | Volume agressor = vendedor | Float |
| `<qtd_neg_direto>` | Negócios diretos | Float |
| `<volume_direto>` | Volume direto | Float |
| `<qtd_neg_indefinido>` | Negócios com agressor indefinido | Float |
| `<volume_indefinido>` | Volume com agressor indefinido | Float |
| `<qtd_neg_RLP>` | Negócios RLP | Float |
| `<volume_RLP>` | Volume RLP | Float |
| `<qtd_neg_leilão>` | Negócios em leilão | Float |
| `<volume_leilão>` | Volume em leilão | Float |

---

## 9. Comandos Utilitários

### 9.1 `GTC` — Get Time Crystal

Obtém o horário corrente do servidor difusor.

**Sintaxe:** `GTC`

**Formato:** `GTC:<YYYYMMDD><HHMMSS>`

**Exemplo:**
```
> GTC
GTC:20170308145946
```

> Note que data e hora vêm **concatenadas, sem separador**.

### 9.2 `QUIT`

Encerra a conexão.

**Sintaxe:** `QUIT`

---

## 10. Tabela de Mensagens de Erro

Toda mensagem de erro tem o formato `E:<código>[:<contexto...>]`.

| Código | Nome | Sintaxe | Significado | Ação recomendada |
|--------|------|---------|-------------|------------------|
| 1 | Comando inválido | `E:1:<Comando>` | Comando não existe | Bug no cliente; log e revisar |
| 2 | Objeto não encontrado | `E:2:<Comando>:<Objeto>:<Complemento>` | Ativo não existe ou inativo | Não tentar reassinar; sinalizar ao usuário |
| 3 | Sem permissão | `E:3:<Comando>:<Objeto>:<Complemento>` | Sem permissão para o serviço/objeto | Pedir upgrade de plano; não reagir com retry |
| 4 | Parâmetro vazio | `E:4:<Comando>` | Parâmetro veio em branco | Bug no cliente |
| 5 | Sem parâmetros | `E:5:<Comando>` | Faltam parâmetros (comando ainda não foi analisado) | Bug no cliente |
| 6 | Duplicate connection (mesmo servidor) | `E:6` | Outra conexão com mesmo usuário foi aberta no mesmo servidor — esta será fechada | Conexão será encerrada; tratar como desconexão |
| 7 | Sem acesso ao sistema | `E:7` | Usuário perdeu acesso, conexão segue ativa | Encerrar gracefully |
| 8 | Duplicate connection (outro servidor) | `E:8` | Outra conexão com mesmo usuário foi aberta em outro servidor — esta será fechada | Conexão será encerrada |
| 9 | Permissões revogadas | `E:9` | Usuário perdeu permissões que garantiam permanência | Encerrar |
| 10 | Parâmetro inválido | `E:10` | Comando válido mas algum parâmetro está incorreto | Bug no cliente; revisar args |
| 11 | Servidor indisponível | `E:11:Server unavailable` | Servidor não aceita novas conexões | Aguardar e retry com backoff |
| 12 | Servidor mudou de host | `E:12:<Host>` | Cliente deve reconectar no novo host indicado | **Reconectar no `<Host>` retornado** |
| 13 | SUID inválido | `E:13:SUID INVALID FORMAT` | Formato do SUID está errado | Bug no cliente |
| 14 | Request ID muito grande | `E:14:REQUEST ID TOO LARGE` | ID excede 14 caracteres | Bug no cliente — limitar IDs |
| 15 | Erro de banco | `E:15:DATABASE ERROR` | Erro interno do servidor | Retry com backoff |
| 16 | Notícia não encontrada | `E:16:<id_requisição>` | Código de notícia inexistente no banco | Avisar ao consumidor |
| 17 | Sem permissão para o serviço | `E:17:<Comando>` | Usuário não tem permissão para esse comando | Não retentar |
| 18 | Quantidade de quotes excedida | `E:18:<Comando>:<Quantidade>` | Tentou assinar mais ativos que o limite | Não retentar; aviso ao usuário |
| 19 | Sem permissão para a quote | `E:19:No permission in quote:<Quote>` | Sem permissão para o ativo específico | Não retentar para esse ativo |

### 10.1 Política de retry sugerida

| Categoria | Códigos | Política |
|-----------|---------|----------|
| Erros de programação (bug no cliente) | 1, 4, 5, 10, 13, 14 | **NÃO retentar.** Log + alerta + fail-fast. |
| Permissão | 3, 9, 17, 18, 19 | Não retentar. Sinalizar ao usuário. |
| Servidor temporário | 11, 15 | Retry com **backoff exponencial** (ex: 1s, 2s, 4s, 8s, max 60s). |
| Migração de servidor | 12 | Reconectar imediatamente no novo host. |
| Desconexão forçada | 6, 7, 8 | Não reconectar automaticamente (pode haver intenção de outra sessão); pedir intervenção. |
| Dado inexistente | 2, 16 | Não retentar; sinalizar ao consumidor. |

---

## 11. Diretrizes de Implementação

### 11.1 Estrutura recomendada de classes/módulos

```
CedroClient
├── Connection           # gerencia socket TCP, handshake, reconexão
├── Reader (thread A)    # drena bytes do socket, push em queue
├── Parser (thread B)    # converte linhas em eventos tipados
├── Dispatcher           # roteia eventos para handlers/callbacks por ativo
├── SubscriptionManager  # mantém estado das subscrições ativas
├── BookState            # mantém estado de livros por ativo (BQT/SAB)
├── ErrorHandler         # implementa política da tabela 10.1
└── Commands             # SQT, BQT, SAB, GQT, NEM, VAP, GTC, GNA, USQ, UBQ, UAB, UQT, UNE, QUIT
```

### 11.2 Modelo de eventos sugerido (linguagem-agnóstico)

```pseudo
Event = Union[
  QuoteUpdate(ativo, campos: Map<int, Value>, timestamp),
  BookAdd(ativo, posição, direção, preço, qtd, corretora, orderId, tipo, ts),
  BookUpdate(ativo, posNova, posAntiga, ..., ts),
  BookDelete(ativo, tipo, direção?, posição?),
  BookInitialComplete(ativo),
  AggBookAdd(ativo, posição, direção, preço, qtd, numOfertas, ts),
  AggBookUpdate(...),
  AggBookDelete(...),
  AggBookInitialComplete(ativo),
  TradeAdd(ativo, ...),
  TradeDelete(ativo, idNeg),
  TradesClear(ativo),
  News(agência, código, data, hora, categoria, título),
  NewsBody(reqId, agência, código, corpo),
  Vap(ativo, preço, ..., período),
  Time(YYYYMMDD, HHMMSS),
  AgencyInfo(símbolo, tipo, código, descrição, tipoAg, cor),
  Error(código, contexto),
]
```

### 11.3 Reconexão

- Manter lista persistente de subscrições ativas.
- Em caso de desconexão (não causada por `E:6`, `E:7`, `E:8`, `E:9`): refazer handshake e **reassinar tudo** que estava ativo.
- Implementar backoff exponencial limitado.
- Em `E:12`, atualizar o host de destino antes de reconectar.

### 11.4 Backpressure / saúde do socket

- **Monitorar tamanho da fila interna** entre Reader e Parser. Se crescer sem parar → o Parser não está acompanhando, gerar alerta.
- O servidor desconecta clientes lentos. **Profilar o caminho hot** (parse) para garantir throughput suficiente.
- Considerar buffer TCP grande (`SO_RCVBUF` aumentado) na configuração do socket.

### 11.5 Limites e validações de entrada do cliente

- `<id_requisição>` para `NEM L`/`NEM N` e `GQT N`: **máximo 14 caracteres** (validar antes de enviar — evitar `E:14`).
- Limite de quantidade de ativos assinados simultaneamente: definido pelo plano do usuário (lança `E:18`).
- Ao montar comandos, **trimar whitespace** e validar que parâmetros obrigatórios estão presentes (evitar `E:4` e `E:5`).

### 11.6 Conversões de tipo importantes

| Campo no protocolo | Conversão recomendada |
|--------------------|----------------------|
| `HHMMSS` | Parsear para tempo local; cuidado com `0` à esquerda omitido. |
| `HHMMSSmmm` | 9 dígitos com milissegundos. |
| `YYYYMMDD` | Parsear para data. Pode vir `00000000` indicando ausência. |
| `YYYYMMDDHHMMSS` | Parsear para datetime. |
| `Float` | Locale-independent (ponto como decimal). |
| `DDMMHHMM` (BQT/SAB) | Atenção: este é dia+mês+hora+minuto, NÃO inclui ano nem segundos. |
| Caractere ETX (0x03) no corpo de notícia | Substituir por `\n`. |

### 11.7 Encoding

A documentação não especifica charset explicitamente, mas dado o conteúdo em português, **ISO-8859-1 (Latin-1)** é a hipótese mais segura por padrão. Validar com o servidor real; pode haver suporte a UTF-8 dependendo da versão. Tratar bytes inválidos com fallback (`replace`) para evitar crash do parser.

### 11.8 Testes mínimos sugeridos

1. **Handshake**: conectar com credenciais válidas e inválidas; verificar timeout em prompts ausentes.
2. **Parser SQT**: alimentar snapshot completo + 10 deltas e validar estado final.
3. **Parser BQT**: aplicar sequência `A/U/D` e validar consistência do livro (sem buracos de posição).
4. **`D:3` total clear** no BQT: validar que limpa ambos os lados.
5. **NEM body**: validar substituição ETX → `\n`.
6. **Erros**: simular cada código de erro e validar política de retry.
7. **Reconexão**: matar conexão TCP no meio e validar reassinatura completa.
8. **Stress**: assinar 100+ ativos populares e medir throughput do parser.

---

## 12. Resumo de Comandos (quick reference)

| Comando | Categoria | Streaming? | Cancelar com |
|---------|-----------|------------|--------------|
| `SQT <ativo>` | Cotação | Sim | `USQ <ativo>` |
| `SQT <ativo> N` | Cotação (snapshot) | Não | — |
| `BQT <ativo>` | Livro detalhado | Sim | `UBQ <ativo>` |
| `SAB <ativo>` | Livro agregado | Sim | `UAB <ativo>` |
| `SAB <ativo> N` | Livro agregado (snapshot) | Não | — |
| `GQT <ativo> S ...` | Trades (subscribe) | Sim | `UQT <ativo>` |
| `GQT <ativo> N ...` | Trades (snapshot) | Não | — |
| `NEM A <agência>` | News (subscribe) | Sim | `UNE <agência>` |
| `NEM L ...` | News (últimas) | Não | — |
| `NEM N ...` | News (corpo) | Não | — |
| `GNA` | Lista agências | Não | — |
| `VAP <ativo> ...` | Volume by price | Não | — |
| `GTC` | Hora do servidor | Não | — |
| `QUIT` | Encerrar | — | — |

---

## 13. Checklist final para implementação

- [ ] Conexão TCP na porta 81 com handshake correto (Software Key → Username → Password → "You are connected").
- [ ] Thread dedicada para drenar o socket (Reader).
- [ ] Thread separada para parsing/processamento (Parser).
- [ ] Fila bounded entre Reader e Parser com métrica de tamanho exposta.
- [ ] Parser que reconhece todos os cabeçalhos (`T`, `B`, `Z`, `V`, `O`, `GNA`, `VAP`, `GTC`, `E`).
- [ ] Tabela completa dos 100+ índices SQT (seção 4.1.1) mapeada para um struct/dataclass tipado.
- [ ] Decodificação dos enums (mercado, tipo de ativo, status, fase, condição de trade, agressor).
- [ ] Manutenção de estado de livro (BQT e SAB) com aplicação correta de `A`/`U`/`D` (tipos 1, 2, 3).
- [ ] Substituição ETX → `\n` em corpos de notícia.
- [ ] Política de erros por código (seção 10.1).
- [ ] Reconexão automática + reassinatura de tudo que estava ativo.
- [ ] Tratamento de `E:12` (mudança de host).
- [ ] Validação de `<id_requisição>` ≤ 14 caracteres.
- [ ] Logging estruturado por evento (útil para debug e replay).
- [ ] Testes unitários do parser para cada tipo de mensagem.
- [ ] Teste de stress validando o requisito de drenagem rápida do socket.