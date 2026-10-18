# Explicação do notebook 04 - Solução do problema

## A ideia geral
Este notebook responde às 5 perguntas de negócio usando **SQL** sobre o modelo estrela (`fato_chamado` +
dimensões) e desenha um gráfico para cada resposta. As tabelas de resposta ficam salvas na nuvem
(`resp_p1_...` a `resp_p5_...`). A discussão de cada resultado é escrita no README, depois de rodar.

## Conceitos usados
- **Média:** soma dividida pela quantidade. É puxada para cima por poucos chamados muito demorados.
- **Mediana:** o valor do meio. Metade dos chamados levou menos que ela, metade levou mais. Representa
  melhor o chamado "típico".
- **P90 (percentil 90):** 90% dos chamados levaram até esse tempo. Mostra o que acontece com os casos lentos.
- **Correlação de Spearman:** número de −1 a 1 que diz se duas medidas crescem juntas. Perto de 0 = sem
  relação; perto de 1 = sobem juntas; negativo = quando uma sobe, a outra desce. Usa as posições (ranking)
  dos valores, então não é distorcida pelos chamados extremos.

## Seção por seção

### Configuração
Prepara a biblioteca de gráficos (`matplotlib`) e cria a função `grafico_barras`, usada em todas as
perguntas para desenhar barras com o valor escrito em cima.

### Pergunta 1 - Primeiro atendimento
- **1a:** média, mediana e P90 do tempo até o primeiro atendimento, por prioridade e no total.
- **1b:** porcentagem de chamados atendidos em menos de 1 h, de 1 a 4 h, de 4 a 24 h, de 1 a 3 dias e em
  mais de 3 dias. Gráfico de barras.

### Pergunta 2 - Tempo de resolução por prioridade
Média, mediana, P90 e mediana em dias do tempo até a resolução, por prioridade. Gráfico da mediana por
prioridade. Só entram chamados com data de resolução registrada.

### Pergunta 3 - Resolvidos e reabertos
- **3a:** quantos chamados foram resolvidos, quantos têm data de resolução e quantos foram reabertos.
- **3b:** quantas vezes foram reabertos (0, 1, 2...) e a porcentagem de reabertos por prioridade. Gráfico.

### Pergunta 4 - Repasses e pessoas envolvidas
- **4a:** média, mediana e máximo de reatribuições, pessoas e grupos por chamado.
- **4b:** porcentagem de chamados com 0, 1, 2, 3 ou 4+ reatribuições e com 0, 1, 2, 3 ou 4+ pessoas.
  Dois gráficos.

### Pergunta 5 - Testando a hipótese
- **5a:** mediana do tempo de resolução para chamados com 0, 1, 2, 3 e 4+ reatribuições, com todos os
  chamados e só com os de prioridade "Moderate". Comparar dentro da mesma prioridade separa o efeito das
  reatribuições do efeito da prioridade, que é exatamente o que a hipótese afirma.
- **5b:** o mesmo para o número de pessoas envolvidas.
- **5c:** correlação de Spearman entre o tempo de resolução e reatribuições, pessoas e grupos, com todas as
  prioridades e só com "Moderate".

### Salvar as respostas
Grava cada tabela de resposta como uma tabela Delta no schema `suporte`, para ficarem persistidas na nuvem.

## Resumo em uma frase (para a apresentação)
"Cada pergunta foi respondida com uma consulta SQL sobre o modelo estrela, usando média, mediana e P90
para os tempos, e a hipótese foi testada comparando o tempo de resolução de chamados com mais e menos
reatribuições dentro da mesma prioridade."
