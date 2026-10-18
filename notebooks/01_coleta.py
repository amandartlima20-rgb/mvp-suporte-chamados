# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Coleta dos dados
# MAGIC
# MAGIC **MVP:** O que faz um chamado de suporte demorar para ser resolvido?
# MAGIC
# MAGIC Este notebook faz a etapa de **coleta**: pega o arquivo CSV baixado do UCI e o guarda na nuvem
# MAGIC (Databricks) como uma tabela do schema do trabalho. Nenhum dado é alterado aqui: a
# MAGIC tabela criada é uma cópia fiel do arquivo original (camada "bronze", ou dado bruto).
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Base | Incident management process enriched event log (UCI, id 498) |
# MAGIC | Licença | CC BY 4.0 |
# MAGIC | Data da coleta | 14/10/2026, 21:45 |
# MAGIC | Arquivo | `incident_event_log.csv` (44,1 MB) |
# MAGIC
# MAGIC ### Como o arquivo chegou ao Databricks
# MAGIC O CSV foi enviado pela interface do Databricks (**New → Add or upload data → Create or modify
# MAGIC table**), que criou a tabela `workspace.default.incident_event_log`. Essa tabela é o nosso ponto de
# MAGIC partida: este notebook a copia, sem alterar os dados, para o schema do trabalho, acrescentando
# MAGIC informações de linhagem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 1 - Criar o local de armazenamento do trabalho
# MAGIC
# MAGIC No Databricks, os dados ficam organizados em três níveis: **catálogo → schema → tabela**.
# MAGIC - O catálogo `workspace` já existe na versão gratuita.
# MAGIC - O upload criou a tabela no schema `default`, que é um schema genérico.
# MAGIC - Criamos um **schema** chamado `suporte` (é como uma "pasta" só para as tabelas deste trabalho).

# COMMAND ----------

# Nomes usados em todo o notebook, definidos uma vez só para evitar erros de digitação
CATALOGO = "workspace"
SCHEMA = "suporte"
ARQUIVO = "incident_event_log.csv"                       # nome do arquivo original baixado do UCI
TABELA_UPLOAD = "workspace.default.incident_event_log"   # tabela criada pelo upload na interface

# spark.sql executa um comando SQL a partir do Python.
# "IF NOT EXISTS" faz o comando não dar erro se o schema já existir (pode rodar de novo sem problema).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
print("Schema pronto:", f"{CATALOGO}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 2 - Conferir a tabela criada pelo upload
# MAGIC
# MAGIC `DESCRIBE DETAIL` mostra os detalhes da tabela: formato, onde está guardada, tamanho e data de
# MAGIC criação. Esta saída serve como **evidência** de que os dados estão persistidos na nuvem (tire um print).

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {TABELA_UPLOAD}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 3 - Ler os dados
# MAGIC
# MAGIC Lemos a tabela do upload para um DataFrame (uma tabela dentro do código).
# MAGIC
# MAGIC Atenção: na criação pela interface, o Databricks **detecta os tipos automaticamente** (por exemplo,
# MAGIC transforma colunas só com números em inteiros). Os nomes e os valores continuam os do arquivo.
# MAGIC Observação: nesta base, valores ausentes aparecem como `?`. Por enquanto eles são mantidos; o
# MAGIC tratamento e a conversão de datas ficam para a etapa de transformação.

# COMMAND ----------

df_bruto = spark.table(TABELA_UPLOAD)

# Mostra os tipos que o Databricks detectou para cada coluna
df_bruto.printSchema()

# Mostra as primeiras 10 linhas para conferir visualmente
display(df_bruto.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 4 - Adicionar informações de linhagem
# MAGIC
# MAGIC Acrescentamos duas colunas que contam **de onde veio** cada linha e **quando** ela foi carregada.
# MAGIC Isso atende ao requisito de linhagem do Catálogo de Dados.

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (
    df_bruto
    # current_timestamp() grava a data e hora em que esta carga foi feita
    .withColumn("_data_carga", F.current_timestamp())
    # lit() cria uma coluna com o mesmo valor em todas as linhas: aqui, o nome do arquivo de origem
    .withColumn("_arquivo_origem", F.lit(ARQUIVO))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 5 - Salvar como tabela na nuvem
# MAGIC
# MAGIC Gravamos os dados como uma **tabela Delta** chamada `incidentes_bronze`.
# MAGIC Delta é o formato padrão do Databricks: os dados ficam persistidos e podem ser consultados com SQL.
# MAGIC O modo `overwrite` substitui a tabela se ela já existir, então o notebook pode ser rodado de novo.

# COMMAND ----------

TABELA_BRONZE = f"{CATALOGO}.{SCHEMA}.incidentes_bronze"

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(TABELA_BRONZE)
)

print("Tabela salva:", TABELA_BRONZE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 6 - Conferir o volume coletado
# MAGIC
# MAGIC Estes números vão para a seção **Coleta** do relatório:
# MAGIC - **linhas (eventos):** cada linha é uma atualização de um chamado;
# MAGIC - **chamados distintos:** a coluna `number` identifica o chamado (ex.: INC0000045);
# MAGIC - **colunas:** quantidade de atributos da base.

# COMMAND ----------

tabela = spark.table(TABELA_BRONZE)

resumo = tabela.agg(
    F.count("*").alias("linhas_eventos"),                # total de linhas
    F.countDistinct("number").alias("chamados_distintos") # total de chamados diferentes
)
display(resumo)

# Colunas originais (sem contar as duas de linhagem que acrescentamos)
colunas_originais = [c for c in tabela.columns if not c.startswith("_")]
print("Quantidade de colunas originais:", len(colunas_originais))
print(colunas_originais)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 7 - Evidência via SQL
# MAGIC
# MAGIC A mesma tabela pode ser consultada em SQL, o que mostra que ela está persistida no catálogo.
# MAGIC Tire um print desta saída para a pasta `/evidencias` do repositório.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT number, incident_state, priority, opened_at, resolved_at, reassignment_count
# MAGIC FROM workspace.suporte.incidentes_bronze
# MAGIC LIMIT 10
