# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Solução do problema: respostas às perguntas de negócio
# MAGIC
# MAGIC **Pergunta norteadora:** O que faz um chamado de suporte demorar para ser resolvido: a prioridade dele,
# MAGIC o quanto ele precisa ser escalado, ou quantas pessoas precisam se envolver até a solução?
# MAGIC
# MAGIC **Hipótese:** chamados que precisam ser escalados ou repassados mais vezes demoram significativamente
# MAGIC mais para serem resolvidos, mesmo quando comparados com chamados de mesma prioridade.
# MAGIC
# MAGIC Cada pergunta tem uma seção com:
# MAGIC 1. a **consulta SQL** no modelo estrela (`fato_chamado` + dimensões), que gera a tabela de resposta;
# MAGIC 2. um **gráfico** simples para visualizar o resultado;
# MAGIC 3. um espaço para a **leitura do resultado**, preenchido no README depois de rodar.
# MAGIC
# MAGIC ### Por que usamos a mediana
# MAGIC Tempos de atendimento têm alguns chamados muito demorados, que puxam a **média** para cima. A
# MAGIC **mediana** é o valor do meio: metade dos chamados levou menos que ela e metade levou mais. Por isso
# MAGIC mostramos as duas, e também o **percentil 90 (P90)**: 90% dos chamados levaram até esse tempo.
# MAGIC
# MAGIC Lembretes da análise de qualidade: 94% dos chamados são de prioridade "3 - Moderate"; 1.556 chamados
# MAGIC fechados não têm data de resolução e ficam fora das perguntas sobre tempo de resolução (R10).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC
# MAGIC `matplotlib` é a biblioteca de gráficos do Python. Definimos um tamanho padrão para os gráficos ficarem
# MAGIC legíveis nos prints.

# COMMAND ----------

import matplotlib.pyplot as plt

plt.rcParams.update({"figure.figsize": (9, 4.5), "font.size": 11})

SCHEMA = "workspace.suporte"

def grafico_barras(df_pandas, x, y, titulo, rotulo_x, rotulo_y):
    """Desenha um gráfico de barras com o valor escrito em cima de cada barra."""
    fig, ax = plt.subplots()
    barras = ax.bar(df_pandas[x].astype(str), df_pandas[y], color="#4C72B0")
    ax.bar_label(barras, fmt="%.1f", padding=3)      # escreve o valor em cima da barra
    ax.set_title(titulo)
    ax.set_xlabel(rotulo_x)
    ax.set_ylabel(rotulo_y)
    ax.spines[["top", "right"]].set_visible(False)  # tira as bordas de cima e da direita
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 1 - Quanto tempo um chamado leva, desde a abertura, até receber o primeiro atendimento?
# MAGIC
# MAGIC **1a.** Estatísticas gerais e por prioridade do tempo até o primeiro atendimento (em horas).
# MAGIC `percentile_approx(coluna, 0.5)` calcula a mediana; `0.9`, o P90.

# COMMAND ----------

p1_prioridade = spark.sql(f"""
    SELECT p.prioridade,
           COUNT(f.horas_ate_primeiro_atendimento)                             AS chamados,
           ROUND(AVG(f.horas_ate_primeiro_atendimento), 1)                     AS media_horas,
           ROUND(percentile_approx(f.horas_ate_primeiro_atendimento, 0.5), 1)  AS mediana_horas,
           ROUND(percentile_approx(f.horas_ate_primeiro_atendimento, 0.9), 1)  AS p90_horas
    FROM {SCHEMA}.fato_chamado f
    JOIN {SCHEMA}.dim_prioridade p ON f.id_prioridade = p.id_prioridade
    GROUP BY p.prioridade

    UNION ALL

    SELECT 'Todas'                                                              AS prioridade,
           COUNT(horas_ate_primeiro_atendimento),
           ROUND(AVG(horas_ate_primeiro_atendimento), 1),
           ROUND(percentile_approx(horas_ate_primeiro_atendimento, 0.5), 1),
           ROUND(percentile_approx(horas_ate_primeiro_atendimento, 0.9), 1)
    FROM {SCHEMA}.fato_chamado
    ORDER BY prioridade
""")
display(p1_prioridade)

# COMMAND ----------

# MAGIC %md
# MAGIC **1b.** Em quanto tempo os chamados são atendidos? Agrupamos em faixas de tempo e mostramos a
# MAGIC porcentagem de chamados em cada faixa.

# COMMAND ----------

p1_faixas = spark.sql(f"""
    SELECT faixa, chamados, ROUND(100.0 * chamados / SUM(chamados) OVER (), 1) AS pct_chamados
    FROM (
        SELECT CASE
                 WHEN horas_ate_primeiro_atendimento < 1  THEN '1. Menos de 1 h'
                 WHEN horas_ate_primeiro_atendimento < 4  THEN '2. De 1 a 4 h'
                 WHEN horas_ate_primeiro_atendimento < 24 THEN '3. De 4 a 24 h'
                 WHEN horas_ate_primeiro_atendimento < 72 THEN '4. De 1 a 3 dias'
                 ELSE '5. Mais de 3 dias'
               END AS faixa,
               COUNT(*) AS chamados
        FROM {SCHEMA}.fato_chamado
        WHERE horas_ate_primeiro_atendimento IS NOT NULL
        GROUP BY 1
    )
    ORDER BY faixa
""")
display(p1_faixas)
grafico_barras(p1_faixas.toPandas(), "faixa", "pct_chamados",
               "P1 - Tempo até o primeiro atendimento", "Faixa de tempo", "% dos chamados")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 2 - Quanto tempo um chamado leva até ser resolvido, e como esse tempo varia por prioridade?
# MAGIC
# MAGIC Mesmas estatísticas da pergunta 1, agora para o tempo até a resolução. Só entram chamados com data
# MAGIC de resolução registrada.

# COMMAND ----------

p2_prioridade = spark.sql(f"""
    SELECT p.prioridade,
           COUNT(f.horas_ate_resolucao)                             AS chamados,
           ROUND(AVG(f.horas_ate_resolucao), 1)                     AS media_horas,
           ROUND(percentile_approx(f.horas_ate_resolucao, 0.5), 1)  AS mediana_horas,
           ROUND(percentile_approx(f.horas_ate_resolucao, 0.9), 1)  AS p90_horas,
           ROUND(percentile_approx(f.horas_ate_resolucao, 0.5) / 24, 1) AS mediana_dias
    FROM {SCHEMA}.fato_chamado f
    JOIN {SCHEMA}.dim_prioridade p ON f.id_prioridade = p.id_prioridade
    GROUP BY p.prioridade

    UNION ALL

    SELECT 'Todas',
           COUNT(horas_ate_resolucao),
           ROUND(AVG(horas_ate_resolucao), 1),
           ROUND(percentile_approx(horas_ate_resolucao, 0.5), 1),
           ROUND(percentile_approx(horas_ate_resolucao, 0.9), 1),
           ROUND(percentile_approx(horas_ate_resolucao, 0.5) / 24, 1)
    FROM {SCHEMA}.fato_chamado
    ORDER BY prioridade
""")
display(p2_prioridade)

grafico_barras(p2_prioridade.filter("prioridade <> 'Todas'").toPandas(), "prioridade", "mediana_horas",
               "P2 - Mediana do tempo até a resolução, por prioridade", "Prioridade", "Horas (mediana)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 3 - Que proporção dos chamados é de fato resolvida, e quantos precisam ser reabertos?
# MAGIC
# MAGIC **3a.** Proporção de chamados resolvidos (estado final "Resolved" ou "Closed", regra R10), quantos têm
# MAGIC data de resolução registrada e quantos foram reabertos ao menos uma vez.

# COMMAND ----------

p3_resumo = spark.sql(f"""
    SELECT COUNT(*)                                                         AS chamados,
           SUM(CASE WHEN foi_resolvido THEN 1 ELSE 0 END)                   AS resolvidos,
           ROUND(100.0 * AVG(CASE WHEN foi_resolvido THEN 1 ELSE 0 END), 2) AS pct_resolvidos,
           SUM(CASE WHEN data_hora_resolucao IS NOT NULL THEN 1 ELSE 0 END) AS com_data_resolucao,
           SUM(CASE WHEN foi_reaberto THEN 1 ELSE 0 END)                    AS reabertos,
           ROUND(100.0 * AVG(CASE WHEN foi_reaberto THEN 1 ELSE 0 END), 2)  AS pct_reabertos
    FROM {SCHEMA}.fato_chamado
""")
display(p3_resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC **3b.** Quantas vezes os chamados reabertos foram reabertos, e a reabertura varia com a prioridade?

# COMMAND ----------

p3_reaberturas = spark.sql(f"""
    SELECT qtd_reaberturas, COUNT(*) AS chamados,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_chamados
    FROM {SCHEMA}.fato_chamado
    GROUP BY qtd_reaberturas
    ORDER BY qtd_reaberturas
""")
display(p3_reaberturas)

p3_prioridade = spark.sql(f"""
    SELECT p.prioridade, COUNT(*) AS chamados,
           ROUND(100.0 * AVG(CASE WHEN f.foi_reaberto THEN 1 ELSE 0 END), 2) AS pct_reabertos
    FROM {SCHEMA}.fato_chamado f
    JOIN {SCHEMA}.dim_prioridade p ON f.id_prioridade = p.id_prioridade
    GROUP BY p.prioridade
    ORDER BY p.prioridade
""")
display(p3_prioridade)
grafico_barras(p3_prioridade.toPandas(), "prioridade", "pct_reabertos",
               "P3 - Chamados reabertos, por prioridade", "Prioridade", "% reabertos")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 4 - Até ser resolvido, quantas vezes um chamado é repassado ou escalado e quantas pessoas diferentes acabam se envolvendo nele?
# MAGIC
# MAGIC **4a.** Médias e medianas de reatribuições, pessoas e grupos envolvidos.

# COMMAND ----------

p4_resumo = spark.sql(f"""
    SELECT ROUND(AVG(qtd_reatribuicoes), 2)                     AS media_reatribuicoes,
           percentile_approx(qtd_reatribuicoes, 0.5)            AS mediana_reatribuicoes,
           MAX(qtd_reatribuicoes)                               AS max_reatribuicoes,
           ROUND(AVG(qtd_pessoas_envolvidas), 2)                AS media_pessoas,
           percentile_approx(qtd_pessoas_envolvidas, 0.5)       AS mediana_pessoas,
           MAX(qtd_pessoas_envolvidas)                          AS max_pessoas,
           ROUND(AVG(qtd_grupos_envolvidos), 2)                 AS media_grupos,
           percentile_approx(qtd_grupos_envolvidos, 0.5)        AS mediana_grupos,
           MAX(qtd_grupos_envolvidos)                           AS max_grupos
    FROM {SCHEMA}.fato_chamado
""")
display(p4_resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC **4b.** Distribuição: quantos chamados tiveram 0, 1, 2, 3 ou 4+ reatribuições, e quantos envolveram
# MAGIC 0, 1, 2, 3 ou 4+ pessoas.

# COMMAND ----------

p4_reatribuicoes = spark.sql(f"""
    SELECT faixa, chamados, ROUND(100.0 * chamados / SUM(chamados) OVER (), 1) AS pct_chamados
    FROM (
        SELECT CASE WHEN qtd_reatribuicoes >= 4 THEN '4+' ELSE CAST(qtd_reatribuicoes AS STRING) END AS faixa,
               COUNT(*) AS chamados
        FROM {SCHEMA}.fato_chamado GROUP BY 1
    ) ORDER BY faixa
""")
display(p4_reatribuicoes)
grafico_barras(p4_reatribuicoes.toPandas(), "faixa", "pct_chamados",
               "P4 - Reatribuições por chamado", "Número de reatribuições", "% dos chamados")

p4_pessoas = spark.sql(f"""
    SELECT faixa, chamados, ROUND(100.0 * chamados / SUM(chamados) OVER (), 1) AS pct_chamados
    FROM (
        SELECT CASE WHEN qtd_pessoas_envolvidas >= 4 THEN '4+' ELSE CAST(qtd_pessoas_envolvidas AS STRING) END AS faixa,
               COUNT(*) AS chamados
        FROM {SCHEMA}.fato_chamado GROUP BY 1
    ) ORDER BY faixa
""")
display(p4_pessoas)
grafico_barras(p4_pessoas.toPandas(), "faixa", "pct_chamados",
               "P4 - Pessoas envolvidas por chamado", "Número de pessoas", "% dos chamados")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 5 - Mais reatribuições e mais pessoas envolvidas significam uma resolução mais lenta?
# MAGIC
# MAGIC Esta pergunta testa a **hipótese**. Comparamos o tempo de resolução entre grupos de chamados com
# MAGIC 0, 1, 2, 3 e 4+ reatribuições (e o mesmo para pessoas envolvidas).
# MAGIC
# MAGIC Para isolar o efeito da prioridade, fazemos a comparação de duas formas:
# MAGIC - com **todos** os chamados;
# MAGIC - só com os chamados **"3 - Moderate"** (94% da base), ou seja, comparando chamados de mesma prioridade.
# MAGIC
# MAGIC **5a.** Tempo de resolução por número de reatribuições.

# COMMAND ----------

p5_reatribuicoes = spark.sql(f"""
    SELECT CASE WHEN f.qtd_reatribuicoes >= 4 THEN '4+' ELSE CAST(f.qtd_reatribuicoes AS STRING) END AS reatribuicoes,
           COUNT(f.horas_ate_resolucao)                                   AS chamados,
           ROUND(percentile_approx(f.horas_ate_resolucao, 0.5), 1)        AS mediana_horas_todas,
           ROUND(AVG(f.horas_ate_resolucao), 1)                           AS media_horas_todas,
           ROUND(percentile_approx(CASE WHEN p.nivel = 3 THEN f.horas_ate_resolucao END, 0.5), 1)
                                                                          AS mediana_horas_moderate
    FROM {SCHEMA}.fato_chamado f
    JOIN {SCHEMA}.dim_prioridade p ON f.id_prioridade = p.id_prioridade
    GROUP BY 1
    ORDER BY 1
""")
display(p5_reatribuicoes)
grafico_barras(p5_reatribuicoes.toPandas(), "reatribuicoes", "mediana_horas_moderate",
               "P5 - Mediana do tempo de resolução x reatribuições (prioridade Moderate)",
               "Número de reatribuições", "Horas (mediana)")

# COMMAND ----------

# MAGIC %md
# MAGIC **5b.** Tempo de resolução por número de pessoas envolvidas.

# COMMAND ----------

p5_pessoas = spark.sql(f"""
    SELECT CASE WHEN f.qtd_pessoas_envolvidas >= 4 THEN '4+' ELSE CAST(f.qtd_pessoas_envolvidas AS STRING) END AS pessoas,
           COUNT(f.horas_ate_resolucao)                                   AS chamados,
           ROUND(percentile_approx(f.horas_ate_resolucao, 0.5), 1)        AS mediana_horas_todas,
           ROUND(AVG(f.horas_ate_resolucao), 1)                           AS media_horas_todas,
           ROUND(percentile_approx(CASE WHEN p.nivel = 3 THEN f.horas_ate_resolucao END, 0.5), 1)
                                                                          AS mediana_horas_moderate
    FROM {SCHEMA}.fato_chamado f
    JOIN {SCHEMA}.dim_prioridade p ON f.id_prioridade = p.id_prioridade
    GROUP BY 1
    ORDER BY 1
""")
display(p5_pessoas)
grafico_barras(p5_pessoas.toPandas(), "pessoas", "mediana_horas_moderate",
               "P5 - Mediana do tempo de resolução x pessoas envolvidas (prioridade Moderate)",
               "Número de pessoas envolvidas", "Horas (mediana)")

# COMMAND ----------

# MAGIC %md
# MAGIC **5c.** Correlação: um número entre −1 e 1 que mede o quanto duas medidas crescem juntas.
# MAGIC Perto de 0 = sem relação; perto de 1 = quando uma sobe, a outra sobe.
# MAGIC Usamos a correlação de **Spearman**, que compara as **posições** (ranking) dos valores em vez dos
# MAGIC valores em si; por isso ela não é distorcida pelos chamados extremamente demorados.

# COMMAND ----------

# Spearman = correlação comum (Pearson) calculada sobre as POSIÇÕES dos valores.
# Passo 1: para cada medida, calcula a posição média de cada chamado no ranking (empates recebem a média
#          das posições que ocupam). Passo 2: calcula corr() entre as posições.
medidas_p5 = ["qtd_reatribuicoes", "qtd_pessoas_envolvidas", "qtd_grupos_envolvidos"]

def posicoes(coluna):
    """SQL que devolve a posição média (ranking) de 'coluna'."""
    return (f"RANK() OVER (ORDER BY {coluna}) + "
            f"(COUNT(*) OVER (PARTITION BY {coluna}) - 1) / 2.0 AS r_{coluna}")

def spearman(filtro, nome_grupo):
    ranks = ", ".join(posicoes(c) for c in ["horas_ate_resolucao"] + medidas_p5)
    consulta = f"""
        WITH base AS (
            SELECT * FROM {SCHEMA}.fato_chamado
            WHERE horas_ate_resolucao IS NOT NULL {filtro}
        ),
        ranqueado AS (SELECT {ranks} FROM base)
        SELECT {", ".join(f"ROUND(corr(r_horas_ate_resolucao, r_{c}), 3) AS {c}" for c in medidas_p5)},
               COUNT(*) AS chamados
        FROM ranqueado
    """
    linha = spark.sql(consulta).first()
    return [(nome_grupo, c, float(linha[c]), int(linha["chamados"])) for c in medidas_p5]

p5_correlacao = spark.createDataFrame(
    spearman("", "Todas as prioridades") + spearman("AND id_prioridade = 3", "Somente Moderate"),
    "grupo string, medida string, spearman_com_horas_resolucao double, chamados long"
)
display(p5_correlacao)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Salvar as respostas
# MAGIC
# MAGIC Gravamos as tabelas de resposta no schema do trabalho. Assim elas ficam persistidas na nuvem e podem
# MAGIC ser consultadas depois (por exemplo, num dashboard).

# COMMAND ----------

respostas = {
    "resp_p1_prioridade": p1_prioridade, "resp_p1_faixas": p1_faixas,
    "resp_p2_prioridade": p2_prioridade,
    "resp_p3_resumo": p3_resumo, "resp_p3_reaberturas": p3_reaberturas, "resp_p3_prioridade": p3_prioridade,
    "resp_p4_resumo": p4_resumo, "resp_p4_reatribuicoes": p4_reatribuicoes, "resp_p4_pessoas": p4_pessoas,
    "resp_p5_reatribuicoes": p5_reatribuicoes, "resp_p5_pessoas": p5_pessoas, "resp_p5_correlacao": p5_correlacao,
}
for nome, df_resp in respostas.items():
    df_resp.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{SCHEMA}.{nome}")
    print("Salva:", nome)
