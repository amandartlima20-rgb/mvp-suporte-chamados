# Explicação do notebook 01 - Coleta

## A ideia geral
O notebook pega os dados do CSV que você baixou do UCI e já enviou ao Databricks, e os guarda numa tabela do trabalho, que é a nuvem
do trabalho. Ele não muda nenhum dado: só copia o arquivo para uma tabela, igualzinho à fonte. Essa
primeira cópia fiel é chamada de **camada bronze** (dado bruto). As limpezas e cálculos (tempo de
resolução, número de pessoas etc.) vêm depois, em outro notebook.

Por que guardar o dado bruto separado? Se algo der errado numa transformação, você sempre pode voltar
ao original sem baixar o arquivo de novo. E o professor consegue ver que o dado está persistido na nuvem.

## Como o Databricks organiza os dados
Pense numa estrutura de pastas com três níveis:

- **Catálogo** (`workspace`): a "gaveta" principal. Já vem pronto na versão gratuita.
- **Schema**: uma pasta dentro do catálogo. O `default` é genérico; o `suporte` é só deste trabalho.
- **Tabela**: dados organizados em linhas e colunas, prontos para consultar com SQL.

O nome completo de algo é sempre `catálogo.schema.nome`, por exemplo `workspace.suporte.incidentes_bronze`.

O upload do CSV pela interface do Databricks (*Create table*) já criou a tabela
`workspace.default.incident_event_log`. O notebook parte dela.

## Célula por célula

### Célula 1 - Criar o local de armazenamento do trabalho
- Define os nomes usados no notebook inteiro (catálogo, schema, nome do arquivo e tabela do upload) em
  variáveis. Assim, se precisar trocar um nome, troca em um lugar só.
- Executa um comando SQL que cria o schema `suporte`.
- O trecho `IF NOT EXISTS` quer dizer "crie só se ainda não existir". Por isso dá para rodar a célula
  várias vezes sem erro.

### Célula 2 - Conferir a tabela criada pelo upload
- `DESCRIBE DETAIL` mostra os detalhes da tabela do upload: formato, local onde está guardada na nuvem,
  tamanho e data de criação.
- Esse print é a **evidência** de que o dado está guardado na nuvem.

### Célula 3 - Ler os dados
- O Spark (o motor de processamento do Databricks) lê a tabela do upload para um `df_bruto` ("df" vem de
  *DataFrame*, que é uma tabela dentro do código).
- `printSchema()` mostra o tipo de cada coluna. Na criação pela interface, o Databricks detectou os tipos
  sozinho (por exemplo, colunas só com números viram inteiros). Vale anotar isso no relatório.
- Mostra as 10 primeiras linhas para você conferir que a leitura deu certo.
- Detalhe da base: valores vazios aparecem como `?`. Eles ficam assim por enquanto e serão tratados
  depois.

### Célula 4 - Adicionar informações de linhagem
"Linhagem" é registrar **de onde veio** o dado e **quando** ele entrou. O trabalho pede isso no
Catálogo de Dados. Aqui o notebook acrescenta duas colunas a cada linha:
- `_data_carga`: a data e a hora em que você rodou a carga.
- `_arquivo_origem`: o nome do arquivo de onde a linha veio.

O `_` no começo do nome é só uma convenção para mostrar que essas colunas não vieram da base original.

### Célula 5 - Salvar como tabela na nuvem
- Grava o `df_bronze` como uma tabela permanente chamada `workspace.suporte.incidentes_bronze`.
- O formato **Delta** é o padrão do Databricks: os dados ficam salvos em disco na nuvem e podem ser
  consultados com SQL, mesmo depois que o cluster desligar.
- `mode("overwrite")`: se a tabela já existir, ela é substituída. Assim, rodar o notebook de novo
  não duplica os dados.

### Célula 6 - Conferir o volume coletado
Conta três coisas, que vão para a seção de Coleta do relatório:
- **Linhas (eventos):** cada linha é **uma atualização** de um chamado, não um chamado. Um mesmo chamado
  aparece várias vezes, uma para cada mudança (abriu, foi atribuído, foi resolvido...).
- **Chamados distintos:** quantos chamados diferentes existem, contando os valores únicos da coluna
  `number` (ex.: INC0000045).
- **Colunas:** quantos atributos a base original tem (sem contar as duas de linhagem).

### Célula 7 - Evidência via SQL
- Consulta a tabela salva usando SQL puro e mostra 10 linhas com as colunas mais importantes para o
  trabalho: número do chamado, estado, prioridade, abertura, resolução e quantidade de reatribuições.
- Prova que a tabela está no catálogo e pode ser consultada.

## Resumo em uma frase 
"Na coleta, o arquivo original foi enviado ao Databricks e copiado, sem alterações, para
uma tabela Delta chamada `incidentes_bronze`, com colunas de linhagem que registram a origem e a data da
carga."
