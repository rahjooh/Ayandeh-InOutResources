import time ,pyspark ,socket ,jaydebeapi
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os import listdir
from os.path import isfile, join


conf = pyspark.SparkConf()
spark = SparkSession.builder.getOrCreate()

actinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Actinfo")
actinfo_DF.repartition(32).createOrReplaceTempView("actinfo")

BllChakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/BLLCHAKAVAK")
BllChakavak_DF.repartition(32).createOrReplaceTempView("BllChakavak")

basicinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/BasicInfo")
basicinfo_DF.repartition(32).createOrReplaceTempView("basicinfo")

CardBon_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardBon")
CardBon_DF.repartition(32).createOrReplaceTempView("CardBon")

CardDebit_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardDebit")
CardDebit_DF.repartition(32).createOrReplaceTempView("CardDebit")

CardGift_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardGift")
CardGift_DF.repartition(32).createOrReplaceTempView("CardGift")

Chakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Chakavak")
Chakavak_DF.repartition(32).createOrReplaceTempView("Chakavak")

Custinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Custinfo")
Custinfo_DF.repartition(32).createOrReplaceTempView("Custinfo")

Incoming_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Incomming")
Incoming_DF.repartition(32).createOrReplaceTempView("Incoming")

Outgoing_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Outgoing")
Outgoing_DF.repartition(32).createOrReplaceTempView("Outgoing")

SHPRTGS_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/SHPRTGS")
SHPRTGS_DF.repartition(32).createOrReplaceTempView("SHPRTGS")

TotalTxn_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/TotalTxn")
TotalTxn_DF.repartition(32).createOrReplaceTempView("TotalTxn")

Satna_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Satna")
Satna_DF.repartition(32).createOrReplaceTempView("Satna")

Card_DF = CardDebit_DF.union(CardBon_DF).union(CardGift_DF)
Card_DF.repartition(6).createOrReplaceTempView("Card")


print(CardDebit_DF.count())
print(CardGift_DF.count())
print(CardBon_DF.count())
print(Card_DF.count())




