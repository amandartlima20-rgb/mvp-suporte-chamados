# Catálogo de Dados

**Origem de todas as tabelas:** base *Incident management process enriched event log* (UCI, CC BY 4.0),
arquivo `incident_event_log.csv`, coletado em 14/10/2026 às 21:45, carregado na tabela bruta
`workspace.suporte.incidentes_bronze`.

Os domínios abaixo são os **valores esperados**. A análise de qualidade vai conferir se os dados reais
respeitam esses limites e registrar o que não respeitar.

## `fato_chamado` — uma linha por chamado (esperado: 24.918 linhas)

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `id_chamado` | texto | Identificador único do chamado | Formato `INC` + 7 dígitos (ex.: INC0000045); único na tabela | Não | `number` |
| `id_prioridade` | numérico (inteiro) | Chave para `dim_prioridade` | 1 a 4 | Não | `priority` da última atualização |
| `id_categoria` | numérico (inteiro) | Chave para `dim_categoria` | Uma chave existente em `dim_categoria` | Sim: categoria não informada | `category` da última atualização |
| `id_canal` | numérico (inteiro) | Chave para `dim_canal` | Uma chave existente em `dim_canal` | Sim: canal não informado | `contact_type` da última atualização |
| `id_data_abertura` | numérico (inteiro) | Chave para `dim_data`, no formato AAAAMMDD | Datas entre 2016 e 2017 | Não | `opened_at` convertido |
| `data_hora_abertura` | data | Momento em que o chamado foi aberto | 2016 a 2017 | Não | `opened_at` convertido de `d/M/yyyy HH:mm` |
| `data_hora_primeiro_atendimento` | data | Primeiro momento em que o chamado saiu do estado "New" (aproximação) | Maior ou igual à abertura | Sim: chamado nunca saiu de "New" | Menor `sys_updated_at` com `incident_state` diferente de "New" |
| `data_hora_resolucao` | data | Momento em que o chamado foi resolvido | Maior ou igual à abertura | Sim: chamado não resolvido | `resolved_at` da última atualização, convertido |
| `horas_ate_primeiro_atendimento` | numérico (decimal) | Horas entre a abertura e o primeiro atendimento | 0 ou mais; valores muito altos serão investigados | Sim: sem primeiro atendimento | `data_hora_primeiro_atendimento` − `data_hora_abertura` |
| `horas_ate_resolucao` | numérico (decimal) | Horas entre a abertura e a resolução | 0 ou mais; valores muito altos serão investigados | Sim: não resolvido | `data_hora_resolucao` − `data_hora_abertura` |
| `foi_resolvido` | booleano | Se o chamado chegou a ser resolvido | Verdadeiro / Falso | Não | Verdadeiro quando `data_hora_resolucao` está preenchida |
| `qtd_reaberturas` | numérico (inteiro) | Quantas vezes o chamado foi reaberto após resolvido | 0 ou mais | Não | Maior `reopen_count` do chamado |
| `foi_reaberto` | booleano | Se o chamado foi reaberto ao menos uma vez | Verdadeiro / Falso | Não | `qtd_reaberturas` > 0 |
| `qtd_reatribuicoes` | numérico (inteiro) | Quantas vezes o chamado foi repassado para outro grupo ou pessoa | 0 ou mais | Não | Maior `reassignment_count` do chamado |
| `qtd_pessoas_envolvidas` | numérico (inteiro) | Pessoas diferentes que foram responsáveis pelo chamado | 0 ou mais | Não | Contagem distinta de `assigned_to` não nulo, em todos os eventos |
| `qtd_grupos_envolvidos` | numérico (inteiro) | Grupos de atendimento diferentes que trataram o chamado | 0 ou mais | Não | Contagem distinta de `assignment_group` não nulo, em todos os eventos |
| `qtd_atualizacoes` | numérico (inteiro) | Quantas vezes o chamado foi atualizado no sistema | 0 ou mais | Não | Maior `sys_mod_count` do chamado |

## `dim_prioridade`

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `id_prioridade` | numérico (inteiro) | Chave da prioridade | 1 a 4 | Não | Número no início de `priority` |
| `prioridade` | categórico | Nome da prioridade | 1 - Critical, 2 - High, 3 - Moderate, 4 - Low | Não | `priority` |
| `nivel` | numérico (inteiro) | Nível para ordenação (1 = mais urgente) | 1 a 4 | Não | Número no início de `priority` |

## `dim_categoria`

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `id_categoria` | numérico (inteiro) | Chave da categoria | 1 ou mais, sem repetição | Não | Gerada na carga |
| `categoria` | categórico | Categoria do chamado, anonimizada na fonte | Textos no formato "Category N" | Não | `category`, sem os valores `?` |

## `dim_canal`

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `id_canal` | numérico (inteiro) | Chave do canal | 1 ou mais, sem repetição | Não | Gerada na carga |
| `canal` | categórico | Canal de abertura do chamado | Phone, Email, Self service, IVR, Direct opening | Não | `contact_type` |

## `dim_data`

| Atributo | Tipo | Descrição | Domínio esperado | Nulo? | Linhagem |
|---|---|---|---|---|---|
| `id_data` | numérico (inteiro) | Chave no formato AAAAMMDD | Datas de abertura existentes na base | Não | Gerada a partir de `opened_at` |
| `data` | data | Dia de abertura | 2016 a 2017 | Não | `opened_at` |
| `ano` | numérico (inteiro) | Ano | 2016 a 2017 | Não | Extraído de `data` |
| `mes` | numérico (inteiro) | Mês | 1 a 12 | Não | Extraído de `data` |
| `dia` | numérico (inteiro) | Dia do mês | 1 a 31 | Não | Extraído de `data` |
| `dia_semana` | categórico | Dia da semana | Segunda a Domingo | Não | Extraído de `data` |

## Transformações aplicadas (resumo da linhagem)
1. O texto `?` vira nulo em todas as colunas.
2. As datas são convertidas de texto (`d/M/yyyy HH:mm`) para data e hora.
3. As várias linhas de cada chamado são agrupadas em uma só, pela coluna `number`.
4. Atributos descritivos (prioridade, categoria, canal) vêm da última atualização do chamado (maior `sys_mod_count`).
5. As medidas de tempo e de contagem são calculadas conforme a coluna "Linhagem" acima.
