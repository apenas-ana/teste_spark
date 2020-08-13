from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import regexp_extract, regexp_replace
import re
import glob

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext.getOrCreate()

# A pasta teste_semantix contém os arquivos de entrada, necessário criar a pasta.
raw_data_files = glob.glob('teste_semantix/*.gz')

base_df = spark.read.text(raw_data_files)

# regex para obter valores - são criadas 5 colunas para o dataframe
base_df_structured = base_df.select(
    regexp_extract('value', r'^\S*', 0).alias('host'),
    regexp_extract('value', r'\[(\d{2}\/\w{3}\/\d{4}).*\]', 1).alias('date'),
    regexp_extract('value', r'"(.*)"', 1).alias('request'),
    regexp_extract('value', r'"\s(\d+)\s(\d+|-)$', 1).alias('http_status'),
    regexp_extract('value', r'(\d+|-)$', 1).cast('integer').alias('bytes')    
    )

# Exibe os 10 primeiros
base_df_structured.show(10, truncate=True)

# hosts únicos são hosts com contador igual a 1
uniqueHostCount = base_df_structured.groupBy('host').count().where("count == 1").count()

# Resposta da questão 1
print('Número de hosts únicos: {}'.format(uniqueHostCount))

lines404 = base_df_structured.where("http_status == 404")
total404 = lines404.groupBy('http_status').count().collect()

# Resposta da questão 2
print('O total de erros 404: {}'.format(total404[0][1]))

top5_hosts404 = lines404.groupBy('host').count().sort(['count'], ascending=False).head(5)

# Resposta da questão 3
print('Os 5 URLs que mais causaram erro 404:')
print(top5_hosts404)

err404_per_day = lines404.groupBy('date').count().sort(['date'], ascending=True).collect()

# Resposta da questão 4
print('Quantidade de erros 404 por dia:')
print(err404_per_day)

# substitui todos bytes que estão igual a '-' por 0 para poder realizar a soma
base_df_structured = base_df_structured.withColumn('bytes', regexp_replace('bytes', '-', '0'))

# Resposta da questão 5
print('O total de bytes retornados:')
print(base_df_structured.groupBy().sum().collect()[0])
