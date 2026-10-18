# Catálogo de Dados

O catálogo segue o conteúdo mínimo exigido pelo documento da disciplina: **Nome do atributo, Tipo,
Descrição, Domínio, Obrigatoriedade e Linhagem**.

**Fonte comum a todas as tabelas:** base *Incident management process enriched event log*, do UCI Machine
Learning Repository (licença CC BY 4.0), arquivo `incident_event_log.csv`, **extraído em 14/10/2026 às
21:45**. Na nuvem, os dados passam por `workspace.default.incident_event_log` (upload) →
`workspace.suporte.incidentes_bronze` (cópia fiel) → `workspace.suporte.incidentes_prata` (eventos
tratados) → tabelas do modelo estrela abaixo.

Os domínios são os **valores esperados**; a análise de qualidade confere se os dados reais os respeitam.

---

## `fato_chamado` — uma linha por chamado (esperado: 24.918 linhas)

| Nome do atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|
| `id_chamado` | Texto | Identificador único do chamado | `INC` + 7 dígitos (ex.: INC0000045); único na tabela | Obrigatório | Origem: coluna `number` do CSV (extraído em 14/10/2026). Transformações: remoção de espaços; agrupamento das linhas de eventos em uma por chamado |
| `id_prioridade` | Numérico | Chave da prioridade do chamado (liga à `dim_prioridade`) | 1 a 4 | Obrigatório | Origem: coluna `priority` (extraído em 14/10/2026). Transformações: valor da última atualização do chamado; troca do texto pela chave da `dim_prioridade` |
| `id_categoria` | Numérico | Chave da categoria (liga à `dim_categoria`) | Uma chave existente em `dim_categoria` | Opcional: nulo significa que o chamado não teve categoria informada | Origem: coluna `category` (extraído em 14/10/2026). Transformações: `?` → nulo; valor da última atualização; troca pela chave da `dim_categoria` |
| `id_canal` | Numérico | Chave do canal de abertura (liga à `dim_canal`) | Uma chave existente em `dim_canal` | Opcional: nulo significa canal não informado | Origem: coluna `contact_type` (extraído em 14/10/2026). Transformações: `?` → nulo; valor da última atualização; troca pela chave da `dim_canal` |
| `id_data_abertura` | Numérico | Chave da data de abertura (liga à `dim_data`), no formato AAAAMMDD | 20160101 a 20171231 | Obrigatório | Origem: coluna `opened_at` (extraído em 14/10/2026). Transformações: texto → data; formatação AAAAMMDD |
| `data_hora_abertura` | Data | Momento em que o chamado foi aberto | 01/01/2016 a 31/12/2017 | Obrigatório | Origem: coluna `opened_at` (extraído em 14/10/2026). Transformações: texto `d/M/yyyy H:mm` → data e hora |
| `data_hora_primeiro_atendimento` | Data | Primeiro momento em que o chamado saiu do estado "New" (aproximação do primeiro atendimento) | Maior ou igual a `data_hora_abertura` | Opcional: nulo significa que o chamado nunca saiu do estado "New" | Origem: colunas `sys_updated_at` e `incident_state` (extraído em 14/10/2026). Transformações: texto → data; menor `sys_updated_at` entre os eventos com estado diferente de "New" |
| `data_hora_resolucao` | Data | Momento em que o chamado foi resolvido | Maior ou igual a `data_hora_abertura` | Opcional: nulo significa chamado não resolvido | Origem: coluna `resolved_at` (extraído em 14/10/2026). Transformações: `?` → nulo; texto → data; valor da última atualização |
| `horas_ate_primeiro_atendimento` | Numérico | Horas entre a abertura e o primeiro atendimento | Mínimo 0; máximo esperado 8.760 (1 ano); acima disso será investigado | Opcional: nulo quando não há primeiro atendimento | Calculado: (`data_hora_primeiro_atendimento` − `data_hora_abertura`) em horas, 2 casas decimais |
| `horas_ate_resolucao` | Numérico | Horas entre a abertura e a resolução | Mínimo 0; máximo esperado 8.760 (1 ano); acima disso será investigado | Opcional: nulo quando o chamado não foi resolvido | Calculado: (`data_hora_resolucao` − `data_hora_abertura`) em horas, 2 casas decimais |
| `foi_resolvido` | Booleano | Se o chamado chegou a ser resolvido | Verdadeiro / Falso | Obrigatório | Calculado: verdadeiro quando `data_hora_resolucao` está preenchida |
| `qtd_reaberturas` | Numérico | Quantas vezes o chamado foi reaberto depois de resolvido | Mínimo 0; máximo esperado 10 | Obrigatório | Origem: coluna `reopen_count` (extraído em 14/10/2026). Transformações: texto → inteiro; maior valor entre os eventos do chamado |
| `foi_reaberto` | Booleano | Se o chamado foi reaberto ao menos uma vez | Verdadeiro / Falso | Obrigatório | Calculado: `qtd_reaberturas` > 0 |
| `qtd_reatribuicoes` | Numérico | Quantas vezes o chamado foi repassado para outro grupo ou pessoa | Mínimo 0; máximo esperado 30 | Obrigatório | Origem: coluna `reassignment_count` (extraído em 14/10/2026). Transformações: texto → inteiro; maior valor entre os eventos do chamado |
| `qtd_pessoas_envolvidas` | Numérico | Quantas pessoas diferentes foram responsáveis pelo chamado | Mínimo 0; máximo esperado 30 | Obrigatório (0 significa que nunca houve responsável atribuído) | Origem: coluna `assigned_to` (extraído em 14/10/2026). Transformações: `?` → nulo; contagem de valores distintos não nulos entre os eventos |
| `qtd_grupos_envolvidos` | Numérico | Quantos grupos de atendimento diferentes trataram o chamado | Mínimo 0; máximo esperado 30 | Obrigatório (0 significa que nunca houve grupo atribuído) | Origem: coluna `assignment_group` (extraído em 14/10/2026). Transformações: `?` → nulo; contagem de valores distintos não nulos entre os eventos |
| `qtd_atualizacoes` | Numérico | Quantas vezes o chamado foi modificado no sistema | Mínimo 0; máximo esperado 150 | Obrigatório | Origem: coluna `sys_mod_count` (extraído em 14/10/2026). Transformações: texto → inteiro; maior valor entre os eventos do chamado |

## `dim_prioridade`

| Nome do atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|
| `id_prioridade` | Numérico | Chave da prioridade | 1 a 4 | Obrigatório | Origem: coluna `priority` (extraído em 14/10/2026). Transformação: número antes do " - " |
| `prioridade` | Categórico | Nome da prioridade | 1 - Critical, 2 - High, 3 - Moderate, 4 - Low | Obrigatório | Origem: coluna `priority` (extraído em 14/10/2026). Transformação: valores distintos |
| `nivel` | Numérico | Nível para ordenação (1 = mais urgente) | 1 a 4 | Obrigatório | Origem: coluna `priority` (extraído em 14/10/2026). Transformação: número antes do " - " |

## `dim_categoria`

| Nome do atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|
| `id_categoria` | Numérico | Chave da categoria | 1 até o número de categorias distintas, sem repetição | Obrigatório | Gerada na carga: numeração sequencial em ordem alfabética da categoria |
| `categoria` | Categórico | Categoria do chamado, anonimizada na fonte | Textos no formato "Category N" | Obrigatório | Origem: coluna `category` (extraído em 14/10/2026). Transformações: `?` descartado; valores distintos |

## `dim_canal`

| Nome do atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|
| `id_canal` | Numérico | Chave do canal | 1 até o número de canais distintos, sem repetição | Obrigatório | Gerada na carga: numeração sequencial em ordem alfabética do canal |
| `canal` | Categórico | Canal de abertura do chamado | Direct opening, Email, IVR, Phone, Self service | Obrigatório | Origem: coluna `contact_type` (extraído em 14/10/2026). Transformações: `?` descartado; valores distintos |

## `dim_data`

| Nome do atributo | Tipo | Descrição | Domínio | Obrigatoriedade | Linhagem |
|---|---|---|---|---|---|
| `id_data` | Numérico | Chave no formato AAAAMMDD | 20160101 a 20171231 | Obrigatório | Gerada na carga a partir de `data` |
| `data` | Data | Dia de abertura de chamados | 01/01/2016 a 31/12/2017 | Obrigatório | Origem: coluna `opened_at` (extraído em 14/10/2026). Transformações: texto → data; valores distintos |
| `ano` | Numérico | Ano | 2016 a 2017 | Obrigatório | Calculado: ano de `data` |
| `mes` | Numérico | Mês | 1 a 12 | Obrigatório | Calculado: mês de `data` |
| `dia` | Numérico | Dia do mês | 1 a 31 | Obrigatório | Calculado: dia de `data` |
| `dia_semana` | Categórico | Dia da semana | Domingo, Segunda, Terça, Quarta, Quinta, Sexta, Sábado | Obrigatório | Calculado: dia da semana de `data` |

---

## Transformações aplicadas (resumo)
1. **R1:** o texto `?` vira nulo em todas as colunas.
2. **R2:** datas convertidas de texto (`d/M/yyyy H:mm`) para data e hora.
3. **R3:** contadores convertidos de texto para inteiro.
4. **R5:** atributos descritivos (prioridade, categoria, canal, resolução) vêm da última atualização do chamado (maior `sys_mod_count`).
5. **R6 a R11:** medidas de tempo e contagem calculadas conforme a coluna "Linhagem".

As regras estão numeradas igual ao notebook `02_carga.py`, onde cada uma é aplicada.
