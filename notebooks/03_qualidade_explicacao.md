# Explicação do notebook 03 - Análise de qualidade dos dados

## A ideia geral
Antes de responder às perguntas, precisamos saber se dá para confiar nos dados. O documento pede uma
análise de qualidade **para cada atributo**, mesmo que nenhum problema seja encontrado. Este notebook
mede os problemas em seis dimensões e guarda tudo numa tabela-resumo (`qualidade_resultado`).

Ele **não altera** nenhum dado: só mede. Com os números em mãos, decidimos no relatório como cada
problema será tratado nas respostas (por exemplo, deixar de fora tempos negativos).

## Célula por célula

### Célula 1 - Configuração
Abre as três tabelas usadas (bronze, prata e fato) e cria a função `registrar`, que anota o resultado de
cada verificação (quantos registros têm o problema e qual a porcentagem) para o resumo final.

### Célula 2 - Completude
Para cada uma das 36 colunas, mostra quantos valores faltam: quantos eram `?` na fonte, quantos nulos
existem depois do tratamento e quantas **falhas de conversão** houve (valores que existiam mas não
viraram data ou número). Falhas de conversão deveriam ser zero; se não forem, há um formato inesperado.

### Célula 2b - Perfil de valores de cada atributo
Para **todas as colunas** (as 36 da base e as da tabela fato), mostra o tipo, quantos valores estão
preenchidos, quantos são distintos e qual o menor e o maior valor. É a "análise de valores por atributo"
que o documento pede, e permite comparar cada coluna com o domínio do Catálogo de Dados.

### Célula 3 - Unicidade
Confere se há eventos repetidos na fonte, se algum chamado aparece duas vezes na fato e se as chaves
das dimensões se repetem. Tudo deve dar zero.

### Célula 4 - Conformidade
- Para os atributos categóricos (prioridade, impacto, urgência, canal, estado), mostra todos os valores
  encontrados e conta os que estão fora da lista do Catálogo de Dados.
- Para os identificadores (número do chamado, categoria, grupo, responsável), confere o formato com um
  padrão de texto. Ex.: `^INC[0-9]{7}$` = "INC" seguido de exatamente 7 dígitos.

### Célula 5 - Consistência
Procura contradições: resolvido antes de abrir, fechado antes de resolver, primeiro atendimento antes da
abertura, estado "resolvido" sem data de resolução (e o contrário), e data de abertura mudando dentro do
mesmo chamado. Também mostra a combinação impacto x urgência x prioridade.

### Célula 6 - Acurácia
Mostra o resumo estatístico de cada medida (mínimo, quartis, mediana, máximo, média) e conta os valores
fora dos limites do catálogo, como tempos negativos ou acima de 1 ano.

**Por que a mediana importa:** em tempos de atendimento, alguns chamados demoram muito mais que os
outros e puxam a média para cima. A mediana (o valor do meio) mostra melhor o chamado "típico".

### Célula 7 - Atualidade
Mostra a primeira e a última data de abertura e quantos chamados foram abertos por mês, para saber qual
período os dados cobrem.

### Célula 8 - Resumo
Junta todas as verificações numa tabela com dimensão, verificação, quantidade e porcentagem, e grava
como `qualidade_resultado`. É o principal print de evidência desta etapa.

## Resumo em uma frase 
"Antes de analisar, verificamos cada atributo em seis dimensões de qualidade (completude, unicidade,
conformidade, consistência, acurácia e atualidade) e registramos quantos registros tinham cada tipo de
problema, para decidir como tratá-los nas respostas."
