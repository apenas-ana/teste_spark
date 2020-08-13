### Qual o objetivo do comando cache em Spark?

É uma técnica de otimização aplicada em DataFrames e Datasets com o intuito de melhorar a performance dos jobs do Spark. A otimização na performance se dá por conta da reutilização de informações que precisam ser computadas repetidamente, pois pensando principalmente em cenários em que se trabalha com bilhões ou trilhões de dados, a performance cai a medida que se recalcula uma mesma informação repetidamente. Como solução para este problema, o comando cache() aloca essa informação na memória, poupando tempo e consumo de hardware.
De maneira mais prática, explicando através de um pseudo-código, salvar uma computação com o comando cache() no Spark, seria mais ou menos assim:
```
variable dataframe = read("dados_ibge.csv")
variable cidade_sp = dataframe.where("cidade" == "São Paulo").cache()
variable bairro_pinheiros = cidade_sp.where("bairro" == "Pinheiros")
```
No exemplo acima, acessos futuros (bairro_pinheiros) ao resultado da filtragem por dados de São Paulo ficarão mais rápidos, pois estão na memória.
Caso não haja espaço para armazenamento na memória, é utilizado o disco.

### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?

Isso se deve principalmente ao fato de que o Spark processa os dados na memória, enquanto o MapReduce faz o processamento lendo e gravando no disco, fazendo com que o Spark possa ser até 100 vezes mais rápido. Um importante fator para esse ganho do Spark na relação tempo/velocidade é o uso do comando cache (explicado na questão anterior).

### Qual a função do SparkContext?

Basicamente, o SparkContext é a primeira coisa que um programa Spark deve criar, pois o SparkContext é utilizado para se conectar ao Spark Cluster e para trabalhar com RDDs. A maioria das operações e funções utilizadas no Spark vêm do SparkContext. Antes de se criar o SparkContext, é necessário criar o SparkConf. 

### O que é RDD?

RDD (Resilient Distributed Dataset) é a representação do Spark para dados, uma coleção de objetos que servem apenas para leitura (imutável) e são distribuídos (daí o Distributed no nome) pelos nós do cluster para obter paralelismo.

Existem duas formas de criar um RDD, carregando um dataset externo e paralelizando uma coleção existente de dados. Exemplificados abaixo:

```bash
val moviesRDD = sc.textFile("path/to/to/the/movies.txt")
val animalsRDD = sc.parallelize(List["cat", "dog", "bird", "human"])
```

É no RDD que os dados são processados.

O Resilient vem do fato de ser tolerante à falhas.

### GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

Apesar de ambos gerarem o mesmo resultado, o groupByKey transfere todo o dataset pela rede, pelo fato de embaralhar todos os pares de chave-valor, enquanto o reduceByKey faz somas locais para cada chave em cada partição, para só depois unir todos esses cálculos para obter um resultado final.

Exemplo para reduceByKey, pensando em duas partições:

partição 1: (a, 1), (a, 1), (b, 1), (b, 1) → (a, 2), (b, 2)

partição 2: (a, 1), (a, 1), (a, 1), (b, 1), (b, 1) → (a, 3), (b, 2)

Juntando os resultados parciais acima para obter o resultado final, teríamos: (a, 5), (b, 4)

Exemplo para groupByKey, pensando em duas partições:

partição 1: (a, 1), (a, 1), (b, 1)

partição 2: (a, 1), (a, 1), (a, 1), (b, 1), (b, 1), (b, 1)

Todos os pares de chave-valor são transportados pela rede sem realizar somas locais antes, então ficaria 1 partição com (a, 1), (a, 1), (a, 1), (a, 1), (a, 1) e outra com (b, 1), (b, 1), (b, 1), (b, 1). E só depois desse transporte é que seria feita a soma para cada chave, transferindo dados desnecessariamente. O resultado final também será (a, 5), (b, 4).

### Explique o que o código Scala abaixo faz.

1. val textFile = sc.textFile("hdfs://...")

Criação de um RDD lendo todas as linhas de um arquivo de texto no formato HDFS (Hadoop Distributed File System) com o método textFile do objeto SparkContext (sc).

2. val counts = textFile.flatMap(line => line.split(" "))

Obtém todas as palavras presentes em cada linha do arquivo ao separar (split) o conteúdo da linha por espaço e armazena em counts.

3. .map(word => (word, 1))

Para cada palavra da coleção, concebe um contador chamado de word inicializado com 1. O contador tem o intuito de contabilizar as ocorrências de cada palavra.

4. .reduceByKey(_ + _)

Reduz todas as ocorrências de uma mesma palavra a um único par de chave-valor (palavra, ocorrências), ou seja, se a palavra música apareceu 3 vezes, então irá gerar um par de chave-valor (música, 3).

5. counts.saveAsTextFile("hdfs://...")

Como passo final, armazena a contabilização de ocorrências de todas as palavras em um arquivo texto no HDFS.
