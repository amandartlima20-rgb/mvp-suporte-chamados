# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Carga e tratamento dos dados (ETL)
# MAGIC
# MAGIC **MVP:** O que faz um chamado de suporte demorar para ser resolvido?
# MAGIC
# MAGIC Este notebook faz o **ETL** (Extração, Transformação e Carga):
# MAGIC
# MAGIC | Etapa | O que acontece aqui |
# MAGIC |---|---|
# MAGIC | **Extração** | Lê a tabela bruta `workspace.suporte.incidentes_bronze`, criada no notebook 01 |
# MAGIC | **Transformação** | Limpa os dados (troca `?` por vazio, converte datas e números) e junta as várias linhas de cada chamado em uma só, calculando os tempos e as contagens |
# MAGIC | **Carga** | Grava as tabelas do Esquema Estrela: `fato_chamado` e as dimensões `dim_prioridade`, `dim_categoria`, `dim_canal` e `dim_data` |
# MAGIC
# MAGIC O fluxo completo do pipeline fica assim:
# MAGIC
# MAGIC `incidentes_bronze` (bruto, 1 linha por evento) → `incidentes_prata` (tratado, 1 linha por evento) → `fato_chamado` + `dim_*` (modelo estrela, 1 linha por chamado)
# MAGIC
# MAGIC Cada **regra de negócio** aplicada está numerada (R1, R2, ...) e explicada na célula onde acontece.
# MAGIC Rode as células na ordem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 1 - Configuração
# MAGIC
# MAGIC Importamos as funções do Spark e definimos os nomes das tabelas em um lugar só.

# COMMAND ----------

from pyspark.sql import functions as F   # funções prontas do Spark (datas, contagens, etc.)
from pyspark.sql import Window           # "janela": permite olhar várias linhas do mesmo chamado juntas

SCHEMA = "workspace.suporte"
TABELA_BRONZE = f"{SCHEMA}.incidentes_bronze"
TABELA_PRATA = f"{SCHEMA}.incidentes_prata"

# Formato das datas no arquivo original. Ex.: "29/2/2016 01:16"
# d = dia (1 ou 2 dígitos), M = mês (1 ou 2 dígitos), yyyy = ano, H = hora, mm = minutos
FORMATO_DATA = "d/M/yyyy H:mm"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 2 - Extração: ler a tabela bruta
# MAGIC
# MAGIC Lemos a tabela bronze e convertemos **todas as colunas para texto**. Na criação pelo upload, o
# MAGIC Databricks adivinhou os tipos sozinho; convertendo tudo para texto, partimos de uma base uniforme e
# MAGIC fazemos nós mesmos as conversões, de forma documentada, nas próximas células.

# COMMAND ----------

bronze = spark.table(TABELA_BRONZE)

# Colunas de linhagem (começam com "_") ficam de fora: elas não fazem parte da base original
colunas_originais = [c for c in bronze.columns if not c.startswith("_")]

# Para cada coluna, aplica cast("string"), ou seja, converte para texto
df = bronze.select([F.col(c).cast("string").alias(c) for c in colunas_originais])

print("Linhas lidas:", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 3 - Transformação: tratar valores ausentes
# MAGIC
# MAGIC **R1 - O texto `?` vira nulo.** Na base original, quando uma informação não existe, aparece o
# MAGIC caractere `?`. Para o Spark, `?` é um texto como outro qualquer, então ele contaria `?` como uma
# MAGIC pessoa ou uma categoria. Trocamos por **nulo** (vazio de verdade), que as funções de contagem
# MAGIC ignoram automaticamente. Também removemos espaços extras no começo e no fim dos textos.

# COMMAND ----------

def limpar_texto(nome_coluna):
    """Remove espaços nas pontas e troca '?' (ou texto vazio) por nulo."""
    valor = F.trim(F.col(nome_coluna))                # trim = tira espaços do começo e do fim
    return (
        F.when(valor.isin("?", ""), None)              # se for "?" ou vazio, vira nulo
         .otherwise(valor)                             # senão, mantém o valor
         .alias(nome_coluna)
    )

df = df.select([limpar_texto(c) for c in df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 4 - Transformação: converter tipos
# MAGIC
# MAGIC **R2 - Datas de texto para data e hora.** As colunas de data estão como texto (`29/2/2016 01:16`).
# MAGIC Convertidas para o tipo *timestamp*, elas permitem calcular diferenças de tempo.
# MAGIC
# MAGIC **R3 - Contadores de texto para número inteiro.** `reassignment_count`, `reopen_count` e
# MAGIC `sys_mod_count` viram inteiros.
# MAGIC
# MAGIC **R4 - Verdadeiro/falso para booleano.** `active`, `made_sla` e `knowledge` viram booleanos.
# MAGIC
# MAGIC Usamos as versões `try_` das conversões: se algum valor estiver fora do formato, ele vira nulo em vez
# MAGIC de travar o notebook. Esses casos serão contados na análise de qualidade.

# COMMAND ----------

colunas_data = ["opened_at", "sys_created_at", "sys_updated_at", "resolved_at", "closed_at"]
colunas_inteiras = ["reassignment_count", "reopen_count", "sys_mod_count"]
colunas_booleanas = ["active", "made_sla", "knowledge", "u_priority_confirmation"]

for c in colunas_data:
    # try_to_timestamp converte o texto usando o formato informado
    df = df.withColumn(c, F.expr(f"try_to_timestamp({c}, '{FORMATO_DATA}')"))

for c in colunas_inteiras:
    # try_cast converte para inteiro; se não for um número, vira nulo
    df = df.withColumn(c, F.expr(f"try_cast({c} AS INT)"))

for c in colunas_booleanas:
    # lower() deixa em minúsculas, para aceitar "true", "TRUE" ou "True"
    df = df.withColumn(c, F.expr(f"try_cast(lower({c}) AS BOOLEAN)"))

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 5 - Carga da camada prata (eventos tratados)
# MAGIC
# MAGIC Gravamos os eventos já limpos na tabela `incidentes_prata`. Ela continua com **uma linha por
# MAGIC evento**, mas agora com tipos corretos e sem `?`. Ter essa camada intermediária ajuda a conferir o
# MAGIC tratamento antes da agregação e será usada na análise de qualidade.

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(TABELA_PRATA)
)

prata = spark.table(TABELA_PRATA)
display(prata.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 6 - Transformação: estado final de cada chamado
# MAGIC
# MAGIC **R5 - O estado final do chamado é a sua última atualização.** Cada chamado aparece em várias linhas.
# MAGIC Para saber a prioridade, a categoria, o canal e a data de resolução **finais**, pegamos a linha com o
# MAGIC maior `sys_mod_count` (número de modificações). Em caso de empate, a de `sys_updated_at` mais recente.
# MAGIC
# MAGIC Como funciona: a *janela* agrupa as linhas de cada chamado (`partitionBy("number")`) e as ordena
# MAGIC da mais recente para a mais antiga. `row_number()` numera essas linhas (1, 2, 3...) e ficamos só
# MAGIC com a número 1.

# COMMAND ----------

janela_ultima = (
    Window
    .partitionBy("number")                                            # um grupo por chamado
    .orderBy(F.col("sys_mod_count").desc(), F.col("sys_updated_at").desc())  # mais recente primeiro
)

ultimo_evento = (
    prata
    .withColumn("ordem", F.row_number().over(janela_ultima))
    .filter(F.col("ordem") == 1)
    .select(
        F.col("number").alias("id_chamado"),
        F.col("priority").alias("prioridade"),
        F.col("category").alias("categoria"),
        F.col("contact_type").alias("canal"),
        F.col("opened_at").alias("data_hora_abertura"),
        F.col("resolved_at").alias("data_hora_resolucao"),
    )
)

print("Chamados (uma linha por chamado):", ultimo_evento.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 7 - Transformação: medidas calculadas com todos os eventos
# MAGIC
# MAGIC Algumas medidas precisam olhar **todas** as linhas do chamado, não só a última:
# MAGIC
# MAGIC - **R6 - Primeiro atendimento (aproximação):** o primeiro momento (`sys_updated_at`) em que o estado
# MAGIC   do chamado é diferente de "New". A base não tem a data exata do primeiro atendimento, por isso
# MAGIC   usamos essa aproximação, registrada como limitação do trabalho.
# MAGIC - **R7 - Pessoas envolvidas:** quantos responsáveis diferentes (`assigned_to`) o chamado teve.
# MAGIC   Grupos envolvidos: o mesmo, com `assignment_group`. Nulos não são contados.
# MAGIC - **R8 - Contadores:** reatribuições, reaberturas e atualizações são o **maior** valor registrado,
# MAGIC   porque esses contadores só aumentam ao longo da vida do chamado.
# MAGIC
# MAGIC `groupBy("number")` junta as linhas de cada chamado e `agg` calcula as medidas de cada grupo.

# COMMAND ----------

medidas = (
    prata
    .groupBy("number")
    .agg(
        # min de sys_updated_at, considerando só as linhas em que o estado não é "New"
        F.min(F.when(F.col("incident_state") != "New", F.col("sys_updated_at")))
            .alias("data_hora_primeiro_atendimento"),
        F.countDistinct("assigned_to").alias("qtd_pessoas_envolvidas"),
        F.countDistinct("assignment_group").alias("qtd_grupos_envolvidos"),
        F.max("reassignment_count").alias("qtd_reatribuicoes"),
        F.max("reopen_count").alias("qtd_reaberturas"),
        F.max("sys_mod_count").alias("qtd_atualizacoes"),
    )
    .withColumnRenamed("number", "id_chamado")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 8 - Transformação: juntar tudo e calcular os tempos
# MAGIC
# MAGIC Juntamos o estado final (célula 6) com as medidas (célula 7) pelo `id_chamado` e calculamos:
# MAGIC
# MAGIC - **R9 - Tempos em horas:** diferença entre duas datas, em segundos, dividida por 3.600.
# MAGIC - **R10 - Resolvido:** o chamado tem data de resolução preenchida.
# MAGIC - **R11 - Reaberto:** o chamado tem pelo menos uma reabertura.

# COMMAND ----------

def horas_entre(inicio, fim):
    """Diferença entre duas datas em horas. Converter timestamp para 'long' dá o valor em segundos."""
    return (F.col(fim).cast("long") - F.col(inicio).cast("long")) / 3600

chamados = (
    ultimo_evento
    .join(medidas, on="id_chamado", how="inner")
    .withColumn("horas_ate_primeiro_atendimento",
                F.round(horas_entre("data_hora_abertura", "data_hora_primeiro_atendimento"), 2))
    .withColumn("horas_ate_resolucao",
                F.round(horas_entre("data_hora_abertura", "data_hora_resolucao"), 2))
    .withColumn("foi_resolvido", F.col("data_hora_resolucao").isNotNull())
    .withColumn("foi_reaberto", F.col("qtd_reaberturas") > 0)
)

display(chamados.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 9 - Carga das dimensões
# MAGIC
# MAGIC Cada dimensão é a lista de valores **distintos** de um atributo, com uma chave numérica (`id_...`).
# MAGIC
# MAGIC - **`dim_prioridade`:** a chave é o próprio número da prioridade (o "1" de "1 - Critical").
# MAGIC - **`dim_categoria`** e **`dim_canal`:** a chave é um número sequencial gerado em ordem alfabética.
# MAGIC - **`dim_data`:** uma linha por dia de abertura, com chave no formato AAAAMMDD (ex.: 20160229).

# COMMAND ----------

def salvar(df_tabela, nome):
    """Grava um DataFrame como tabela Delta no schema do trabalho, substituindo a versão anterior."""
    (df_tabela.write.format("delta").mode("overwrite")
        .option("overwriteSchema", True).saveAsTable(f"{SCHEMA}.{nome}"))
    print(f"{nome}: {spark.table(f'{SCHEMA}.{nome}').count()} linhas")

# --- dim_prioridade ---
dim_prioridade = (
    chamados.select("prioridade").where(F.col("prioridade").isNotNull()).distinct()
    # split quebra "1 - Critical" no " - "; o primeiro pedaço ("1") vira o número
    .withColumn("nivel", F.split("prioridade", " - ").getItem(0).cast("int"))
    .withColumn("id_prioridade", F.col("nivel"))
    .select("id_prioridade", "prioridade", "nivel")
)

def dimensao_sequencial(coluna, nome_id):
    """Cria uma dimensão com os valores distintos de 'coluna' e uma chave 1, 2, 3... em ordem alfabética."""
    return (
        chamados.select(coluna).where(F.col(coluna).isNotNull()).distinct()
        .withColumn(nome_id, F.row_number().over(Window.orderBy(coluna)))
        .select(nome_id, coluna)
    )

dim_categoria = dimensao_sequencial("categoria", "id_categoria")
dim_canal = dimensao_sequencial("canal", "id_canal")

# --- dim_data ---
# dayofweek: 1 = domingo, 2 = segunda, ..., 7 = sábado
nomes_dias = F.create_map(*[F.lit(x) for par in [
    (1, "Domingo"), (2, "Segunda"), (3, "Terça"), (4, "Quarta"),
    (5, "Quinta"), (6, "Sexta"), (7, "Sábado")] for x in par])

dim_data = (
    chamados.select(F.to_date("data_hora_abertura").alias("data"))
    .where(F.col("data").isNotNull()).distinct()
    .withColumn("id_data", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("dia_semana", nomes_dias[F.dayofweek("data")])
    .select("id_data", "data", "ano", "mes", "dia", "dia_semana")
)

salvar(dim_prioridade, "dim_prioridade")
salvar(dim_categoria, "dim_categoria")
salvar(dim_canal, "dim_canal")
salvar(dim_data, "dim_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 10 - Carga da tabela fato
# MAGIC
# MAGIC Trocamos os textos (prioridade, categoria, canal, data) pelas chaves das dimensões, usando
# MAGIC `join`. O join é do tipo `left`: se um chamado não tiver categoria, ele continua na fato, só que
# MAGIC com `id_categoria` nulo, em vez de ser descartado.

# COMMAND ----------

fato_chamado = (
    chamados
    .join(spark.table(f"{SCHEMA}.dim_prioridade").select("id_prioridade", "prioridade"), "prioridade", "left")
    .join(spark.table(f"{SCHEMA}.dim_categoria"), "categoria", "left")
    .join(spark.table(f"{SCHEMA}.dim_canal"), "canal", "left")
    .withColumn("id_data_abertura", F.date_format("data_hora_abertura", "yyyyMMdd").cast("int"))
    .select(
        "id_chamado", "id_prioridade", "id_categoria", "id_canal", "id_data_abertura",
        "data_hora_abertura", "data_hora_primeiro_atendimento", "data_hora_resolucao",
        "horas_ate_primeiro_atendimento", "horas_ate_resolucao",
        "foi_resolvido", "qtd_reaberturas", "foi_reaberto",
        "qtd_reatribuicoes", "qtd_pessoas_envolvidas", "qtd_grupos_envolvidos", "qtd_atualizacoes",
    )
)

salvar(fato_chamado, "fato_chamado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 11 - Conferência da carga
# MAGIC
# MAGIC Verificações para garantir que a carga está **correta** (vão para o relatório e para as evidências):
# MAGIC 1. A fato deve ter **uma linha por chamado**: o total de linhas deve ser igual ao de chamados
# MAGIC    distintos da tabela bruta (24.918).
# MAGIC 2. `id_chamado` não pode se repetir na fato.
# MAGIC 3. Todo chamado deve ter prioridade e data de abertura (chaves obrigatórias).

# COMMAND ----------

fato = spark.table(f"{SCHEMA}.fato_chamado")
chamados_bronze = spark.table(TABELA_BRONZE).select("number").distinct().count()

conferencia = fato.agg(
    F.count("*").alias("linhas_fato"),
    F.countDistinct("id_chamado").alias("chamados_distintos_fato"),
    F.sum(F.col("id_prioridade").isNull().cast("int")).alias("sem_prioridade"),
    F.sum(F.col("id_data_abertura").isNull().cast("int")).alias("sem_data_abertura"),
).withColumn("chamados_na_bronze", F.lit(chamados_bronze))

display(conferencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 12 - Evidência via SQL
# MAGIC
# MAGIC Consulta que usa o modelo estrela completo: junta a fato com a dimensão de prioridade.
# MAGIC Tire um print para a pasta `/evidencias`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.prioridade,
# MAGIC        COUNT(*)                          AS chamados,
# MAGIC        ROUND(AVG(f.horas_ate_resolucao), 1) AS media_horas_resolucao,
# MAGIC        ROUND(AVG(f.qtd_reatribuicoes), 2)   AS media_reatribuicoes
# MAGIC FROM workspace.suporte.fato_chamado f
# MAGIC JOIN workspace.suporte.dim_prioridade p ON f.id_prioridade = p.id_prioridade
# MAGIC GROUP BY p.prioridade
# MAGIC ORDER BY p.prioridade
