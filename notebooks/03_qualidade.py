# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Análise de qualidade dos dados
# MAGIC
# MAGIC **MVP:** O que faz um chamado de suporte demorar para ser resolvido?
# MAGIC
# MAGIC Antes de responder às perguntas, precisamos saber se os dados são confiáveis. Este notebook examina
# MAGIC **cada atributo** nas seis dimensões de qualidade citadas no documento da disciplina:
# MAGIC
# MAGIC | Dimensão | Pergunta que responde | Célula |
# MAGIC |---|---|---|
# MAGIC | **Completude** | Quantos valores estão faltando em cada coluna? | 2 |
# MAGIC | **Unicidade** | Existem registros ou chaves repetidas? | 3 |
# MAGIC | **Conformidade** | Os valores respeitam o domínio declarado no Catálogo de Dados? | 4 |
# MAGIC | **Consistência** | Existem contradições entre atributos (ex.: resolvido antes de abrir)? | 5 |
# MAGIC | **Acurácia** | Existem valores implausíveis (ex.: tempos negativos ou enormes)? | 6 |
# MAGIC | **Atualidade** | Qual período os dados cobrem? | 7 |
# MAGIC
# MAGIC A célula 8 junta tudo numa tabela-resumo, gravada como `qualidade_resultado`.
# MAGIC Este notebook **só mede** os problemas; a decisão de como tratá-los é discutida no README, com base
# MAGIC nos números encontrados aqui.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 1 - Configuração
# MAGIC
# MAGIC Usamos três tabelas criadas nos notebooks anteriores:
# MAGIC - `incidentes_bronze`: dado bruto, para ver como os valores vieram da fonte;
# MAGIC - `incidentes_prata`: eventos tratados (sem `?`, com tipos convertidos);
# MAGIC - `fato_chamado`: uma linha por chamado, com as medidas usadas nas perguntas.

# COMMAND ----------

from pyspark.sql import functions as F

SCHEMA = "workspace.suporte"
bronze = spark.table(f"{SCHEMA}.incidentes_bronze")
prata = spark.table(f"{SCHEMA}.incidentes_prata")
fato = spark.table(f"{SCHEMA}.fato_chamado")

total_eventos = prata.count()
total_chamados = fato.count()
print("Eventos:", total_eventos, "| Chamados:", total_chamados)

# Lista onde cada verificação vai guardar seu resultado, para o resumo da célula 8
resultados = []

def registrar(dimensao, verificacao, qtd_problemas, total):
    """Guarda o resultado de uma verificação: quantos registros têm o problema e qual a porcentagem."""
    resultados.append((dimensao, verificacao, int(qtd_problemas), int(total),
                       round(100.0 * qtd_problemas / total, 2) if total else 0.0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 2 - Completude: valores ausentes por coluna
# MAGIC
# MAGIC Para **cada uma das 36 colunas** da base, contamos:
# MAGIC - `interrogacoes_na_fonte`: quantas vezes o valor original era `?` (ausente na fonte);
# MAGIC - `nulos_apos_tratamento`: quantos nulos existem na camada prata;
# MAGIC - `falhas_de_conversao`: nulos que **não** vieram de `?`, ou seja, valores que existiam mas não
# MAGIC   puderam ser convertidos (ex.: uma data em formato errado). O esperado é zero.
# MAGIC
# MAGIC A tabela sai ordenada da coluna mais incompleta para a mais completa.

# COMMAND ----------

colunas = [c for c in prata.columns]

# Na bronze: conta '?' ou vazio em cada coluna (convertendo para texto antes de comparar)
contagem_fonte = bronze.agg(*[
    F.sum(F.trim(F.col(c).cast("string")).isin("?", "").cast("int")).alias(c) for c in colunas
]).first().asDict()

# Na bronze: conta nulos "de verdade" (sem valor algum)
nulos_fonte = bronze.agg(*[
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in colunas
]).first().asDict()

# Na prata: conta nulos depois do tratamento
nulos_prata = prata.agg(*[
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in colunas
]).first().asDict()

linhas = []
for c in colunas:
    ausentes_fonte = (contagem_fonte[c] or 0) + (nulos_fonte[c] or 0)
    nulos = nulos_prata[c] or 0
    linhas.append((c, ausentes_fonte, nulos, nulos - ausentes_fonte,
                   round(100.0 * nulos / total_eventos, 2)))

completude = spark.createDataFrame(
    linhas, "coluna string, interrogacoes_na_fonte long, nulos_apos_tratamento long, "
            "falhas_de_conversao long, pct_ausente double"
).orderBy(F.col("pct_ausente").desc())

display(completude)

# Registra no resumo as colunas usadas nas perguntas
for c in ["number", "opened_at", "resolved_at", "priority", "assigned_to", "assignment_group",
          "reassignment_count", "reopen_count", "category", "contact_type"]:
    registrar("Completude", f"Ausentes em {c} (eventos)", nulos_prata[c] or 0, total_eventos)
registrar("Completude", "Falhas de conversão (todas as colunas)",
          sum(max(l[3], 0) for l in linhas), total_eventos * len(colunas))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 2b - Perfil de valores de cada atributo
# MAGIC
# MAGIC O documento pede uma **análise de valores por atributo**, mesmo quando não há problemas. Esta célula
# MAGIC monta um perfil de **todas as colunas** da camada prata (36 atributos da fonte) e da tabela fato
# MAGIC (atributos do modelo): tipo, quantidade de valores preenchidos, de valores distintos, menor e maior
# MAGIC valor. Assim dá para comparar cada atributo com o domínio do Catálogo de Dados.

# COMMAND ----------

def perfil(tabela, nome_tabela):
    """Calcula, para cada coluna, tipo, preenchidos, distintos, mínimo e máximo."""
    linhas = []
    for campo in tabela.schema.fields:
        c = campo.name
        r = tabela.agg(
            F.count(c).alias("preenchidos"),               # count ignora nulos
            F.countDistinct(c).alias("distintos"),
            F.min(F.col(c).cast("string")).alias("minimo") if campo.dataType.simpleString() == "string"
                else F.min(c).cast("string").alias("minimo"),
            F.max(F.col(c).cast("string")).alias("maximo") if campo.dataType.simpleString() == "string"
                else F.max(c).cast("string").alias("maximo"),
        ).first()
        linhas.append((nome_tabela, c, campo.dataType.simpleString(),
                       r["preenchidos"], r["distintos"], r["minimo"], r["maximo"]))
    return linhas

perfil_atributos = spark.createDataFrame(
    perfil(prata, "incidentes_prata") + perfil(fato, "fato_chamado"),
    "tabela string, atributo string, tipo string, preenchidos long, distintos long, "
    "minimo string, maximo string"
)
display(perfil_atributos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 3 - Unicidade: registros e chaves repetidas
# MAGIC
# MAGIC - Eventos idênticos em todas as colunas (já tratados pela R12 na carga).
# MAGIC - `id_chamado` repetido na tabela fato (deve ser zero: uma linha por chamado).
# MAGIC - Chaves repetidas nas dimensões (cada chave deve aparecer uma vez só).

# COMMAND ----------

eventos_repetidos = bronze.drop("_data_carga", "_arquivo_origem").count() - \
                    bronze.drop("_data_carga", "_arquivo_origem").distinct().count()
chamados_repetidos = total_chamados - fato.select("id_chamado").distinct().count()

registrar("Unicidade", "Eventos idênticos na fonte", eventos_repetidos, total_eventos)
registrar("Unicidade", "id_chamado repetido na fato", chamados_repetidos, total_chamados)

for dim, chave in [("dim_prioridade", "id_prioridade"), ("dim_categoria", "id_categoria"),
                   ("dim_canal", "id_canal"), ("dim_data", "id_data")]:
    t = spark.table(f"{SCHEMA}.{dim}")
    repetidas = t.count() - t.select(chave).distinct().count()
    registrar("Unicidade", f"Chave repetida em {dim}", repetidas, t.count())

print("Eventos idênticos:", eventos_repetidos, "| id_chamado repetidos:", chamados_repetidos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 4 - Conformidade: valores dentro do domínio do catálogo
# MAGIC
# MAGIC **Atributos categóricos:** listamos todos os valores encontrados e quantas vezes aparecem. Valores
# MAGIC fora da lista esperada no Catálogo de Dados são contados como problema.
# MAGIC
# MAGIC **Identificadores:** conferimos o formato com uma *expressão regular* (um padrão de texto).
# MAGIC Por exemplo, `^INC[0-9]{7}$` significa "começa com INC e tem exatamente 7 dígitos".

# COMMAND ----------

dominios = {
    "priority": ["1 - Critical", "2 - High", "3 - Moderate", "4 - Low"],
    "impact": ["1 - High", "2 - Medium", "3 - Low"],
    "urgency": ["1 - High", "2 - Medium", "3 - Low"],
    "contact_type": ["Direct opening", "Email", "IVR", "Phone", "Self service"],
    "incident_state": ["New", "Active", "Awaiting User Info", "Awaiting Problem",
                       "Awaiting Vendor", "Awaiting Evidence", "Resolved", "Closed"],
}

for coluna, esperados in dominios.items():
    print(f"\n=== {coluna} ===")
    display(prata.groupBy(coluna).count().orderBy(coluna))
    fora = prata.filter(F.col(coluna).isNotNull() & ~F.col(coluna).isin(esperados)).count()
    registrar("Conformidade", f"{coluna} fora do domínio", fora, total_eventos)

padroes = {
    "number": r"^INC[0-9]{7}$",
    "category": r"^Category [0-9]+$",
    "assignment_group": r"^Group [0-9]+$",
    "assigned_to": r"^Resolver [0-9]+$",
}
for coluna, padrao in padroes.items():
    fora = prata.filter(F.col(coluna).isNotNull() & ~F.col(coluna).rlike(padrao)).count()
    registrar("Conformidade", f"{coluna} fora do formato {padrao}", fora, total_eventos)
    print(f"{coluna}: {fora} valores fora do formato")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 5 - Consistência: contradições entre atributos
# MAGIC
# MAGIC Verificamos situações que não deveriam acontecer:
# MAGIC 1. Chamado resolvido **antes** de ser aberto.
# MAGIC 2. Chamado fechado **antes** de ser resolvido.
# MAGIC 3. Primeiro atendimento **antes** da abertura.
# MAGIC 4. Chamado cujo estado final é "Resolved" ou "Closed", mas **sem** data de resolução.
# MAGIC 5. Chamado com data de resolução, mas cujo estado final **não** é "Resolved" nem "Closed".
# MAGIC 6. A data de abertura mudando entre os eventos de um mesmo chamado.
# MAGIC
# MAGIC Também mostramos a combinação impacto x urgência x prioridade, porque a prioridade costuma ser
# MAGIC derivada das outras duas.

# COMMAND ----------

from pyspark.sql import Window

# Estado final de cada chamado (mesma regra R5 da carga: última atualização)
janela = Window.partitionBy("number").orderBy(F.col("sys_mod_count").desc(), F.col("sys_updated_at").desc())
final = (prata.withColumn("ordem", F.row_number().over(janela)).filter("ordem = 1"))

c1 = final.filter(F.col("resolved_at") < F.col("opened_at")).count()
c2 = final.filter(F.col("closed_at") < F.col("resolved_at")).count()
c3 = fato.filter(F.col("data_hora_primeiro_atendimento") < F.col("data_hora_abertura")).count()
c4 = final.filter(F.col("incident_state").isin("Resolved", "Closed") & F.col("resolved_at").isNull()).count()
c5 = final.filter(F.col("resolved_at").isNotNull() & ~F.col("incident_state").isin("Resolved", "Closed")).count()
c6 = prata.groupBy("number").agg(F.countDistinct("opened_at").alias("n")).filter("n > 1").count()

registrar("Consistência", "Resolvido antes de abrir", c1, total_chamados)
registrar("Consistência", "Fechado antes de resolver", c2, total_chamados)
registrar("Consistência", "Primeiro atendimento antes da abertura", c3, total_chamados)
registrar("Consistência", "Estado final Resolved/Closed sem data de resolução", c4, total_chamados)
registrar("Consistência", "Com data de resolução, mas estado final não resolvido", c5, total_chamados)
registrar("Consistência", "Data de abertura diferente entre eventos do mesmo chamado", c6, total_chamados)

# Combinações de impacto, urgência e prioridade no estado final
display(final.groupBy("impact", "urgency", "priority").count().orderBy("impact", "urgency"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 6 - Acurácia: valores implausíveis nas medidas
# MAGIC
# MAGIC Primeiro, um resumo estatístico de cada medida da fato: mínimo, quartis (25%, 50% = mediana, 75%),
# MAGIC máximo e média. Depois contamos os valores fora dos limites declarados no Catálogo de Dados:
# MAGIC tempos negativos ou acima de 1 ano (8.760 h) e contadores acima do máximo esperado.

# COMMAND ----------

medidas = ["horas_ate_primeiro_atendimento", "horas_ate_resolucao", "qtd_reaberturas",
           "qtd_reatribuicoes", "qtd_pessoas_envolvidas", "qtd_grupos_envolvidos", "qtd_atualizacoes"]

# summary calcula as estatísticas pedidas para cada coluna
display(fato.select(medidas).summary("count", "min", "25%", "50%", "75%", "max", "mean"))

limites = {   # (mínimo, máximo) esperados, conforme o Catálogo de Dados
    "horas_ate_primeiro_atendimento": (0, 8760),
    "horas_ate_resolucao": (0, 8760),
    "qtd_reaberturas": (0, 10),
    "qtd_reatribuicoes": (0, 30),
    "qtd_pessoas_envolvidas": (0, 30),
    "qtd_grupos_envolvidos": (0, 30),
    "qtd_atualizacoes": (0, 150),
}
for coluna, (minimo, maximo) in limites.items():
    abaixo = fato.filter(F.col(coluna) < minimo).count()
    acima = fato.filter(F.col(coluna) > maximo).count()
    registrar("Acurácia", f"{coluna} abaixo de {minimo}", abaixo, total_chamados)
    registrar("Acurácia", f"{coluna} acima de {maximo}", acima, total_chamados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 7 - Atualidade: período coberto pelos dados
# MAGIC
# MAGIC Mostra a primeira e a última data de abertura e quantos chamados foram abertos em cada mês.
# MAGIC Chamados fora do período esperado (2016 a 2017) são contados como problema.

# COMMAND ----------

display(fato.agg(F.min("data_hora_abertura").alias("primeira_abertura"),
                 F.max("data_hora_abertura").alias("ultima_abertura")))

display(fato.groupBy(F.date_format("data_hora_abertura", "yyyy-MM").alias("mes"))
            .count().orderBy("mes"))

fora_periodo = fato.filter(~F.year("data_hora_abertura").between(2016, 2017)).count()
registrar("Atualidade", "Abertura fora de 2016-2017", fora_periodo, total_chamados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Célula 8 - Resumo da qualidade
# MAGIC
# MAGIC Junta todas as verificações numa tabela só, com a dimensão, a verificação, a quantidade de registros
# MAGIC com problema e a porcentagem. A tabela é gravada como `qualidade_resultado` e serve de evidência.
# MAGIC **Tire um print desta saída** (e da célula 2).

# COMMAND ----------

resumo = spark.createDataFrame(
    resultados,
    "dimensao string, verificacao string, qtd_problemas long, total long, pct double"
)
(resumo.write.format("delta").mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{SCHEMA}.qualidade_resultado"))

display(resumo)
