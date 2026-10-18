# Evidências

Capturas de tela que comprovam a execução do pipeline no Databricks.

| Arquivo | O que mostra | Etapa |
|---|---|---|
| `coleta_detalhe_tabela_upload.png` | Detalhes da tabela criada pelo upload (formato Delta, persistida no catálogo) | Coleta |
| `coleta_volume_coletado.png` | Volume coletado: 141.712 linhas, 24.918 chamados, 36 colunas | Coleta |
| `coleta_consulta_sql_bronze.png` | Consulta SQL à tabela bruta `workspace.suporte.incidentes_bronze` | Coleta |
| `carga_conferencia.png` | Conferência da carga: 24.918 chamados na fato, sem perdas nem repetições | Carga |
| `carga_consulta_sql_estrela.png` | Consulta SQL juntando a fato com a dimensão de prioridade | Carga |
| `carga_job_tarefas.png` | Job `pipeline_suporte` com as 4 tarefas: coleta → carga → qualidade → análise | Carga |
| `carga_job_execucao.png` | Execução do Job completo concluída com sucesso (4 min 38 s) | Carga |
| `carga_conferencia_duplicados.png` | Conferência da carga: 0 eventos duplicados removidos | Carga |
| `qualidade_resumo.png` / `.csv` | Resumo das 47 verificações de qualidade | Qualidade |
| `qualidade_completude.png` / `.csv` | Valores ausentes por coluna | Qualidade |
| `qualidade_perfil_atributos.png` / `.csv` | Perfil de valores de cada atributo | Qualidade |
| `qualidade_dominio_estado.png` | Valores de `incident_state` (inclui o inválido `-100`) | Qualidade |
| `qualidade_dominio_prioridade_impacto.png` | Valores de `priority` e `impact` | Qualidade |
| `qualidade_dominio_urgencia_canal.png` | Valores de `urgency` e `contact_type` | Qualidade |
| `qualidade_resumo_pos_tratamento.csv` | Resumo da qualidade após R6, R10 e R13 (tempos negativos: 2 → 0) | Qualidade |
| `analise_p1_prioridade.csv` / `analise_p1_faixas.csv` | Tempo até o primeiro atendimento, por prioridade e por faixa | Análise P1 |
| `analise_p1_grafico_faixas.png` | Gráfico das faixas de tempo até o primeiro atendimento | Análise P1 |
| `analise_p2_prioridade.csv` | Tempo até a resolução por prioridade (média, mediana, P90) | Análise P2 |
| `analise_p2_grafico_prioridade.png` | Gráfico da mediana da resolução por prioridade | Análise P2 |
| `analise_p3_resumo.csv` / `analise_p3_reaberturas.csv` / `analise_p3_prioridade.csv` | Resolvidos, reabertos e reaberturas por prioridade | Análise P3 |
| `analise_p3_grafico_reabertos.png` | Gráfico de % reabertos por prioridade | Análise P3 |
| `analise_p4_resumo.csv` / `analise_p4_reatribuicoes.csv` / `analise_p4_pessoas.csv` | Reatribuições, pessoas e grupos por chamado | Análise P4 |
| `analise_p4_grafico_reatribuicoes.png` / `analise_p4_grafico_pessoas.png` | Gráficos da distribuição de reatribuições e de pessoas | Análise P4 |
| `analise_p5_reatribuicoes.csv` / `analise_p5_pessoas.csv` | Mediana da resolução por reatribuições e por pessoas | Análise P5 |
| `analise_p5_correlacao.csv` | Correlação de Spearman com o tempo de resolução | Análise P5 |
| `analise_p5_grafico_reatribuicoes.png` / `analise_p5_grafico_pessoas.png` | Gráficos da mediana da resolução (prioridade Moderate) | Análise P5 |
| `custo_preco_jobs_serverless.png` | Preço oficial do Jobs Serverless (US$ 0,35 por DBU), outubro de 2026 | Custo de operação |
