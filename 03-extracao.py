import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType

sc = pyspark.SparkContext(master = "spark://node5:7077", appName='extracao')
spark = SparkSession(sc)

#Criando dataframes a partir das tabelas CSVs
df16 = spark.read.csv('hdfs://node5:9000/refined/csv/base_sinasc_2016_v1.csv', header=True, encoding='ISO-8859-1')
df17 = spark.read.csv('hdfs://node5:9000/refined/csv/base_sinasc_2017_v1.csv', header=True, encoding='ISO-8859-1')
df18 = spark.read.csv('hdfs://node5:9000/refined/csv/base_sinasc_2018_v1.csv', header=True, encoding='ISO-8859-1')
df19 = spark.read.csv('hdfs://node5:9000/refined/csv/base_sinasc_2019_v1.csv', header=True, encoding='ISO-8859-1')

#Base 2016
#Criando coluna com o ano de nascimento
df16 = df16.withColumn('ANO', F.substring(F.col('DTNASC'), 5,4))
#Selecionando apenas as colunas de interesse
df16 = df16.select('CONTADOR','APGAR1', 'APGAR5', 'RACACOR', 'ANO', 'SEXO')

#Base 2017
#Criando coluna com o ano de nascimento
df17 = df17.withColumn('ANO', F.substring(F.col('DTNASC'), 5,4))
#Selecionando apenas as colunas de interesse
df17 = df17.select('CONTADOR','APGAR1', 'APGAR5', 'RACACOR', 'ANO', 'SEXO')

#Base 2018
#Criando coluna com o ano de nascimento
df18 = df18.withColumn('ANO', F.substring(F.col('DTNASC'), 5,4))
#Selecionando apenas as colunas de interesse
df18 = df18.select('CONTADOR','APGAR1', 'APGAR5', 'RACACOR', 'ANO', 'SEXO')

#Base 2019
#Criando coluna com o ano de nascimento
df19 = df19.withColumn('ANO', F.substring(F.col('DTNASC'), 5,4))
#Selecionando apenas as colunas de interesse
df19 = df19.select('CONTADOR','APGAR1', 'APGAR5', 'RACACOR', 'ANO', 'SEXO')

#Gravar no HDFS
df16.repartition(1).write.csv('hdfs://node5:9000/refined/csv/base_sinasc_2016_recortada_v1.csv',header='True')
df17.repartition(1).write.csv('hdfs://node5:9000/refined/csv/base_sinasc_2017_recortada_v1.csv',header='True')
df18.repartition(1).write.csv('hdfs://node5:9000/refined/csv/base_sinasc_2018_recortada_v1.csv',header='True')
df19.repartition(1).write.csv('hdfs://node5:9000/refined/csv/base_sinasc_2019_recortada_v1.csv',header='True')

#Criando dataframe único
df = df16.unionByName(df17)
df = df.unionByName(df18)
df = df.unionByName(df19)

#TABELA FINAL EXPORTADA PARA CSV E GRAVADA NO HDFS
df.repartition(1).write.csv('hdfs://node5:9000/refined/csv/base_sinasc_2016_a_2019_recortada_v1.csv',header='True')
