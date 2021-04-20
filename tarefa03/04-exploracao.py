import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType

sc = pyspark.SparkContext(master = "spark://node5:7077", appName='exploracao')
spark = SparkSession(sc)


#Criando dataframes a partir das tabelas CSVs
df = spark.read.csv('hdfs://node5:9000/refined/csv/base_sinasc_2016_a_2019_recortada_v1.csv', header=True, encoding='ISO-8859-1')

#Conversão de Dados
df = df.withColumn("APGAR1", F.col("APGAR1").astype(IntegerType()))
df = df.withColumn("APGAR5", F.col("APGAR5").astype(IntegerType()))

#Qual o APGAR1 médio de cada ano?
print('1-Qual o APGAR1 médio de cada ano?')
df1=df.groupBy('ANO').mean('APGAR1').orderBy('ANO')
df1.show()

#Qual o APGAR5 médio de cada ano?
print('2-Qual o APGAR5 médio de cada ano?')
df2=df.groupBy('ANO').mean('APGAR5').orderBy('ANO')
df2.show()

#Qual o APGAR1 médio de cada ano para cada sexo?
print('3-Qual o APGAR1 médio de cada ano para cada sexo?')
df3=df.where(F.col('SEXO')!=0).groupBy('ANO','SEXO').mean('APGAR1').orderBy('ANO')
df3.show()

#Qual o APGAR1 médio global para cada sexo?
print('4-Qual o APGAR1 médio global para cada sexo?')
df4=df.where(F.col('SEXO')!=0).groupBy('SEXO').mean('APGAR1')
df4.show()

#Qual o APGAR5 médio de cada ano para cada sexo?
print('5-Qual o APGAR5 médio de cada ano para cada sexo?')
df5=df.where(F.col('SEXO')!=0).groupBy('ANO','SEXO').mean('APGAR5').orderBy('ANO')
df5.show()

#Qual o APGAR5 médio global para cada sexo?
print('6-Qual o APGAR5 médio global para cada sexo?')
df6=df.where(F.col('SEXO')!=0).groupBy('SEXO').mean('APGAR5')
df6.show()

#Qual o APGAR1 médio de cada ano para cada sexo e raça?
print('7-Qual o APGAR1 médio de cada ano para cada sexo e raça?')
df7=df.filter(F.col('RACACOR') != 'null').where(F.col('SEXO')!=0).groupBy('ANO','SEXO','RACACOR').mean('APGAR1').orderBy('ANO','SEXO','RACACOR')
df7.show()

#Qual o APGAR1 médio global para cada sexo e raça?
print('8-Qual o APGAR1 médio global para cada sexo e raça?')
df8=df.filter(F.col('RACACOR') != 'null').where(F.col('SEXO')!=0).groupBy('SEXO','RACACOR').mean('APGAR1').orderBy('SEXO','RACACOR')
df8.show()


