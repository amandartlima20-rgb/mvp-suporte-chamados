# Explicação do notebook 02 - Carga e tratamento (ETL)

## A ideia geral
O notebook 01 guardou os dados **como vieram**: uma linha por evento, tudo como texto, com `?` onde
falta informação. Este notebook faz o **tratamento** e monta o **Esquema Estrela** desenhado na
modelagem. Ao final, existe uma tabela `fato_chamado` com **uma linha por chamado** (24.918) e as
medidas prontas para responder às perguntas, cercada de quatro dimensões.

O caminho dos dados:

| Camada | Tabela | Uma linha por | Situação |
|---|---|---|---|
| Bronze | `incidentes_bronze` | evento | Dado bruto, igual ao arquivo |
| Prata | `incidentes_prata` | evento | Dado limpo: sem `?`, com datas e números no tipo certo |
| Ouro | `fato_chamado` + `dim_*` | chamado | Modelo estrela, com tempos e contagens calculados |

Cada regra de negócio tem um código (R1 a R11), o mesmo usado no Catálogo de Dados.

## Célula por célula

### Célula 1 - Configuração
Importa as ferramentas do Spark e define os nomes das tabelas e o **formato das datas** do arquivo
(`d/M/yyyy H:mm`, por exemplo `29/2/2016 01:16`).

### Célula 2 - Extração
Lê a tabela bronze e transforma **todas as colunas em texto**. No upload, o Databricks adivinhou os tipos
sozinho; partindo de tudo como texto, as conversões ficam sob nosso controle e documentadas.

### Célula 3 - Valores ausentes (R1)
Troca `?` por **nulo**. Isso é importante porque, para o computador, `?` é um texto como outro qualquer:
sem essa troca, `?` seria contado como uma pessoa a mais ou uma categoria a mais. Os nulos são ignorados
pelas contagens.

### Célula 4 - Tipos (R2, R3, R4)
- Datas: de texto para data e hora, para poder calcular diferenças de tempo.
- Contadores (reatribuições, reaberturas, modificações): de texto para número inteiro.
- Campos verdadeiro/falso: de texto para booleano.

As conversões usam a versão `try_`: se algum valor estiver fora do padrão, ele vira nulo em vez de dar
erro. Esses casos aparecem depois na análise de qualidade.

### Célula 4b - Deduplicação (R12)
Procura linhas **idênticas em todas as colunas**, que seriam o mesmo evento registrado duas vezes, e
mantém só uma de cada. Mostra quantas linhas foram removidas. Se o resultado for zero, isso também é
informação útil: mostra que a base não tinha eventos repetidos.

### Célula 5 - Camada prata
Grava os eventos já limpos na tabela `incidentes_prata`. Ainda é uma linha por evento, mas agora com os
tipos corretos. Essa tabela também será usada na análise de qualidade.

### Célula 6 - Estado final do chamado (R5)
Cada chamado tem várias linhas. Para saber a prioridade, a categoria, o canal e a data de resolução
**finais**, o notebook pega a **última atualização** de cada chamado (a de maior `sys_mod_count`).

Como: uma *janela* separa as linhas por chamado, ordena da mais recente para a mais antiga e numera
(1, 2, 3...). Fica só a linha número 1.

### Célula 7 - Medidas que olham todos os eventos (R6, R7, R8)
- **Primeiro atendimento (R6):** o primeiro momento em que o chamado entrou num estado válido diferente
  de "New". É uma aproximação, porque a base não tem essa data exata. O estado inválido `-100` é ignorado.
- **Pessoas e grupos envolvidos (R7):** quantos responsáveis diferentes o chamado teve ao longo da vida.
- **Contadores (R8):** o maior valor de reatribuições, reaberturas e modificações, já que esses números
  só aumentam com o tempo.

### Célula 8 - Juntar e calcular os tempos (R9, R10, R11)
Une o estado final com as medidas e calcula:
- horas até o primeiro atendimento e horas até a resolução (diferença entre datas, em horas);
- se o chamado foi resolvido (estado final "Resolved" ou "Closed") (R10);
- tempos negativos, que são impossíveis, viram nulo (R13).
- se foi reaberto (pelo menos uma reabertura).

### Célula 9 - Dimensões
Cria as quatro dimensões, cada uma com a lista de valores distintos e uma chave numérica:
- `dim_prioridade`: chave é o número da prioridade (1 a 4);
- `dim_categoria` e `dim_canal`: chave sequencial em ordem alfabética;
- `dim_data`: um dia por linha, com ano, mês, dia e dia da semana, chave no formato AAAAMMDD.

### Célula 10 - Tabela fato
Troca os textos pelas chaves das dimensões e grava a `fato_chamado`. O join do tipo `left` garante que
um chamado sem categoria continua na tabela (com a chave vazia) em vez de sumir.

### Célula 11 - Conferência
Verifica que a carga está correta: a fato deve ter o mesmo número de chamados da tabela bruta (24.918),
sem repetição de `id_chamado`, e todo chamado precisa ter prioridade e data de abertura. Também mostra quantos eventos duplicados a célula 4b removeu.

### Célula 12 - Evidência via SQL
Uma consulta que usa o modelo completo (fato + dimensão de prioridade) e já dá uma prévia das respostas:
chamados, média de horas até a resolução e média de reatribuições por prioridade. 

## Resumo em uma frase 
"Na carga, os eventos brutos foram limpos (ausentes, datas e números), agrupados em uma linha por chamado
com os tempos e contagens calculados, e gravados num Esquema Estrela com uma tabela fato e quatro
dimensões, conferindo que nenhum chamado foi perdido."
