import time ,pyspark ,socket ,jaydebeapi
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os import listdir
from os.path import isfile, join

date = '971005'
date1 ='971004'
conf = pyspark.SparkConf()
spark = SparkSession.builder.getOrCreate()
print('\n\n\n\n\n\n\n\n\n\n\n\n')
#
CardBon_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardBon")
CardBon_DF = CardBon_DF.withColumn('CARDNO', rtrim(CardBon_DF.CARDNO))
CardBon_DF = CardBon_DF.withColumn('CARDNO', ltrim(CardBon_DF.CARDNO))
CardBon_DF.createOrReplaceTempView("CardBon")

CardDebit_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardDebit")
CardDebit_DF = CardDebit_DF.withColumn('CARDNO', rtrim(CardDebit_DF.CARDNO))
CardDebit_DF = CardDebit_DF.withColumn('CARDNO', ltrim(CardDebit_DF.CARDNO))
CardDebit_DF.createOrReplaceTempView("CardDebit")

CardGift_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardGift")
CardGift_DF = CardGift_DF.withColumn('CARDNO', rtrim(CardGift_DF.CARDNO))
CardGift_DF = CardGift_DF.withColumn('CARDNO', ltrim(CardGift_DF.CARDNO))
CardGift_DF = CardGift_DF.withColumn('CUSTNO', lit(None).cast(StringType()))
CardGift_DF.createOrReplaceTempView("CardGift")

Card_DF = CardDebit_DF.union(CardBon_DF).union(CardGift_DF)
Card_DF.repartition(6).createOrReplaceTempView("Card")



#
#
#
# Incoming_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Incomming")
# Incoming_DF = Incoming_DF.withColumn('RecAccNo', ltrim(Incoming_DF.RecAccNo))
# Incoming_DF = Incoming_DF.withColumn('RecAccNo', rtrim(Incoming_DF.RecAccNo))
# Incoming_DF = Incoming_DF.withColumn('TrackID', ltrim(Incoming_DF.TrackID))
# Incoming_DF = Incoming_DF.withColumn('TrackID', substring(Incoming_DF.TrackID,0,6))
# Incoming_DF = Incoming_DF.withColumn('RecAccNo', substring('RecAccNo', -13, 13))
# Incoming_DF = Incoming_DF.withColumnRenamed("RecAccNo","Acno")
# Incoming_DF = Incoming_DF.withColumnRenamed("SenderBank","Bank")
# Incoming_DF = Incoming_DF.filter(col('TrackID') == date )
# Incoming_DF = Incoming_DF.filter(col('Acno') != '0300893205004')
# Incoming_DF = Incoming_DF.groupby(['Acno', 'Bank']).agg(sum(col('Amount')).alias('amount'))
# Incoming_DF = Incoming_DF.withColumn('Branch', lit(None).cast(StringType()))
# Incoming_DF = Incoming_DF.withColumn('source', lit('paya').cast(StringType()))
# Incoming_DF = Incoming_DF.withColumn('type', lit('I').cast(StringType()))
# '''Incoming_DF = Incoming_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
# Incoming_DF.createOrReplaceTempView("Incoming")
# print('**')
# print(Incoming_DF.take(1))
# print('**')
#
actinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Actinfo")
actinfo_DF = actinfo_DF.withColumn('ACNO', rtrim(actinfo_DF.ACNO))
actinfo_DF = actinfo_DF.withColumn('Acno', ltrim(actinfo_DF.ACNO))
actinfo_DF = actinfo_DF.withColumn('CUSTNO', rtrim(actinfo_DF.CUSTNO))
actinfo_DF = actinfo_DF.withColumn('Custno', ltrim(actinfo_DF.CUSTNO))
actinfo_DF.createOrReplaceTempView("actinfo")

#
# Outgoing_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Outgoing")
# print (Outgoing_DF.head())
# Outgoing_DF = Outgoing_DF.withColumn('SenderAccNo', ltrim(Outgoing_DF.SenderAccNo))
# Outgoing_DF = Outgoing_DF.withColumn('SenderAccNo', rtrim(Outgoing_DF.SenderAccNo))
# Outgoing_DF = Outgoing_DF.withColumn('TraceId', ltrim(Outgoing_DF.TraceId))
# Outgoing_DF = Outgoing_DF.withColumn('TraceId', rtrim(Outgoing_DF.TraceId))
# Outgoing_DF = Outgoing_DF.withColumn('SenderAccNo', substring('SenderAccNo', -13, 13))
# Outgoing_DF = Outgoing_DF.withColumnRenamed("SenderAccNo","Acno")
# Outgoing_DF = Outgoing_DF.withColumnRenamed("SenderBank","Bank")
# '''Outgoing_DF = Outgoing_DF.filter(col('TraceId') == date )'''
# Outgoing_DF = Outgoing_DF.filter(col('Acno') != '0300893205004')
# Outgoing_DF = Outgoing_DF.groupby(['Acno', 'Bank']).agg(sum(col('Amount')).alias('amount'))
# Outgoing_DF = Outgoing_DF.withColumn('Branch', lit(None).cast(StringType()))
# Outgoing_DF = Outgoing_DF.withColumn('source', lit('paya').cast(StringType()))
# Outgoing_DF = Outgoing_DF.withColumn('type', lit('I').cast(StringType()))
# '''Outgoing_DF = Outgoing_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
# Outgoing_DF.createOrReplaceTempView("Outgoing")
#
# print('*  Outgoing_DF  *')
# print(Outgoing_DF.take(1))
# print('**')


Chakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Chakavak")
Chakavak_DF = Chakavak_DF.withColumn('BenefSheba', rtrim(Chakavak_DF.BenefSheba))
Chakavak_DF = Chakavak_DF.withColumn('BenefSheba', substring('BenefSheba', -13, 13))
Chakavak_DF = Chakavak_DF.withColumnRenamed("BenefSheba","Acno")
Chakavak_DF = Chakavak_DF.withColumn('BenefBrCode',concat(lit('0000'), col('BenefBrCode')))
Chakavak_DF = Chakavak_DF.withColumn('BenefBrCode', substring('BenefBrCode', -4, 4))
Chakavak_DF = Chakavak_DF.withColumnRenamed("BenefBrCode","Branch")
Chakavak_DF = Chakavak_DF.withColumnRenamed("OrderBankName","Bank")
Chakavak_DF = Chakavak_DF.withColumn('OrderTrackNo', rtrim(Chakavak_DF.OrderTrackNo))
Chakavak_DF = Chakavak_DF.withColumn('OrderTrackNo', ltrim(Chakavak_DF.OrderTrackNo))

#       Chakavak non-ramzdar voroodi

# Chakavakin_DF = Chakavak_DF.filter(col('Hisdate') == date1 )
# print(2,Chakavakin_DF.take(1))
# Chakavakin_DF = Chakavakin_DF.filter(col('BenefBankCode') == 'AYBKIRTHXXX' )
# print(3,Chakavakin_DF.take(1))
# Chakavakin_DF = Chakavakin_DF.filter(col('ChqFinalStat').like("%تا%") )
# print(4,Chakavakin_DF.take(1))
# Chakavakin_DF = Chakavakin_DF.filter(~col('ChqType').like("%رمز%") )
# print(5,Chakavakin_DF.take(1))
# Chakavakin_DF = Chakavakin_DF.filter(col('BenefMeliCode') != '10320894878')
# print(6,Chakavakin_DF.take(1))
# Chakavakin_DF = Chakavakin_DF.groupby(['Bank', 'Branch','Acno']).agg(sum(col('Amount')).alias('amount'))
# Chakavakin_DF = Chakavakin_DF.withColumn('source', lit('chakavak_non').cast(StringType()))
# Chakavakin_DF = Chakavakin_DF.withColumn('type', lit('I').cast(StringType()))
# '''Chakavakin_DF = Chakavakin_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
# Chakavakin_DF.createOrReplaceTempView("Chakavakin")
#
# print('*  Chakavakin_DF  *')
# print(Chakavakin_DF.take(1))
# print('**')



#       Chakavak non-ramzdar khorooji



# BllChakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/BLLCHAKAVAK")
# BllChakavak_DF = BllChakavak_DF.withColumn('Acc', rtrim(BllChakavak_DF.Acc))
# BllChakavak_DF = BllChakavak_DF.withColumn('Acc', ltrim(BllChakavak_DF.Acc))
# BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', rtrim(BllChakavak_DF.OptionInfo1))
# BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', ltrim(BllChakavak_DF.OptionInfo1))
# BllChakavak_DF.createOrReplaceTempView("BllChakavak")
# ch = Chakavak_DF.join (BllChakavak_DF , Chakavak_DF.OrderTrackNo == BllChakavak_DF.OptionInfo1, how='left')
# ch = ch.withColumnRenamed("Acno","BenefSheba")
# ch = ch.join (actinfo_DF ,ch.Acc == actinfo_DF.Acno,how='left')
# print(ch.take(10) ,'\n\n\n\n\n\n\n\n\n\n\n')
# '''ch = ch.filter(col('Hisdate') == date1 )'''
# ch = ch.filter(col('BenefBankCode') != 'AYBKIRTHXXX')
# ch = ch.filter(col('ChqFinalStat').like('%تا%'))
# ch = ch.filter(~col('ChqType').like("%رمز%") )
# ch = ch.filter(~col('BenefName').like("%بانک%") )
# ch = ch.filter(~col('BenefName').like("%بانك%") )
# ch = ch.filter(~col('BenefName').like("%خزانه%") )
# ch = ch.filter(~col('BenefName').like("%مبادلات%") )
# ch = ch.filter(col('OrderBankBranch') != '201')
# ch = ch.withColumn('OrderBankBranch',concat(lit('0000'), col('OrderBankBranch')))
# ch = ch.withColumn('OrderBankBranch', substring('OrderBankBranch', -4, 4))
# print(ch.filter(isnull(ch.Acc)).count())
# print(ch.count())
# ###ch = ch.filter(col('Acc') != '030089325004') # ???????????????????????????????????????????????????????????????????????????????????????????????????????
# print(ch.head())
# ch1 = ch.groupby(['BenefBankCode', 'OrderBankBranch','BenefMeliCode','Acc']).agg(sum(col('Amount')).alias('amount'))
# ch = ch.withColumn("Acno", when(isnull(ch.Acc),ch.BenefMeliCode).otherwise(ch.Acc))
# ch = ch.select("OrderTrackNo","Custno","Acno","BenefMeliCode","OrderBankBranch","BenefBankCode","TranAtmX","BenefName")
# ch = ch.withColumn('Branch', lit(None).cast(StringType()))
# ch = ch.withColumn('source', lit('chakavak_non').cast(StringType()))
# ch = ch.withColumn('type', lit('O').cast(StringType()))
# print(ch.head())



##                  Chakavak ramzdar voroodi

# '''Chakavak_DF = Chakavakin_DF.filter(col('Hisdate') == date1 )'''
# Chakavak_DF = Chakavak_DF.filter(col('BenefBankCode') == 'AYBKIRTHXXX' )
# Chakavak_DF = Chakavak_DF.filter(col('ChqFinalStat').like("%تا%") )
# Chakavak_DF = Chakavak_DF.filter(col('ChqType').like("%رمز%") )
# Chakavak_DF = Chakavak_DF.filter(col('BenefMeliCode') != '10320894878')
# Chakavak_DF = Chakavak_DF.groupby(['Bank', 'Branch','Acno']).agg(sum(col('Amount')).alias('amount'))
# Chakavak_DF = Chakavak_DF.withColumn('source', lit('chakavak_rz').cast(StringType()))
# Chakavak_DF = Chakavak_DF.withColumn('type', lit('I').cast(StringType()))
# '''Chakavak_DF = Chakavak_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
# Chakavak_DF.createOrReplaceTempView("ChakavakinRamzdar")
#
# print('*  Chakavak_DF  *')
# print(Chakavak_DF.take(1))
# print('**')



#       Chakavak non-ramzdar khorooji



# BllChakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/BLLCHAKAVAK")
# BllChakavak_DF = BllChakavak_DF.withColumn('Acc', rtrim(BllChakavak_DF.Acc))
# BllChakavak_DF = BllChakavak_DF.withColumn('Acc', ltrim(BllChakavak_DF.Acc))
# BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', rtrim(BllChakavak_DF.OptionInfo1))
# BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', ltrim(BllChakavak_DF.OptionInfo1))
# BllChakavak_DF.createOrReplaceTempView("BllChakavak")
# ch = Chakavak_DF.join (BllChakavak_DF , Chakavak_DF.OrderTrackNo == BllChakavak_DF.OptionInfo1, how='left')
# ch = ch.withColumnRenamed("Acno","BenefSheba")
# ch = ch.join (actinfo_DF ,ch.Acc == actinfo_DF.Acno,how='left')
# print(ch.take(10) ,'\n\n\n\n\n\n\n\n\n\n\n')
# '''ch = ch.filter(col('Hisdate') == date1 )'''
# ch = ch.filter(col('BenefBankCode') != 'AYBKIRTHXXX')
# ch = ch.filter(col('ChqFinalStat').like('%تا%'))
# ch = ch.filter(col('ChqType').like("%رمز%") )
# ch = ch.filter(~col('BenefName').like("%بانک%") )
# ch = ch.filter(~col('BenefName').like("%بانك%") )
# ch = ch.filter(~col('BenefName').like("%خزانه%") )
# ch = ch.filter(~col('BenefName').like("%مبادلات%") )
# ch = ch.filter(col('OrderBankBranch') != '201')
# ch = ch.withColumn('OrderBankBranch',concat(lit('0000'), col('OrderBankBranch')))
# ch = ch.withColumn('OrderBankBranch', substring('OrderBankBranch', -4, 4))
# print(ch.filter(isnull(ch.Acc)).count())
# print(ch.count())
# ### ch = ch.filter(col('Acc') != '030089325004') # ???????????????????????????????????????????????????????????????????????????????????????????????????????
# print(ch.head())
# ch1 = ch.groupby(['BenefBankCode', 'OrderBankBranch','BenefMeliCode','Acc']).agg(sum(col('Amount')).alias('amount'))
# ch = ch.withColumn("Acno", when(isnull(ch.Acc),ch.BenefMeliCode).otherwise(ch.Acc))
# ch = ch.select("OrderTrackNo","Custno","Acno","BenefMeliCode","OrderBankBranch","BenefBankCode","TranAtmX","BenefName")
# ch = ch.withColumn('Branch', lit(None).cast(StringType()))
# ch = ch.withColumn('source', lit('chakavak_rz').cast(StringType()))
# ch = ch.withColumn('type', lit('O').cast(StringType()))
# print(ch.head())

###              satna vorodi

# basicinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/BasicInfo")
# basicinfo_DF = basicinfo_DF.withColumn('ValueCode', rtrim(basicinfo_DF.ValueCode))
# basicinfo_DF = basicinfo_DF.withColumn('ValueCode', ltrim(basicinfo_DF.ValueCode))
# basicinfo_DF = basicinfo_DF.filter(col('FieldName')=='BIC_CODE')
# basicinfo_DF = basicinfo_DF.filter(col('TableName')=='SATNA')
# basicinfo_DF.createOrReplaceTempView("basicinfo")
#
#
# Satna_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Satna")
# Satna_DF = Satna_DF.withColumn('OrderOfSwiftCode', rtrim(Satna_DF.OrderOfSwiftCode))
# Satna_DF = Satna_DF.withColumn('OrderOfSwiftCode', ltrim(Satna_DF.OrderOfSwiftCode))
# Satna_DF = Satna_DF.withColumn('BeneficiaryCommentShaba', rtrim(Satna_DF.BeneficiaryCommentShaba))
# Satna_DF = Satna_DF.withColumn('BeneficiaryCommentShaba', ltrim(Satna_DF.BeneficiaryCommentShaba))
# Satna_DF = Satna_DF.withColumn('BeneficiaryCommentShaba', substring('BeneficiaryCommentShaba', -13, 13))
# Satna_DF.createOrReplaceTempView("Satna")
#
# Satnab = Satna_DF.join (basicinfo_DF ,Satna_DF.OrderOfSwiftCode == basicinfo_DF.ValueCode,how='left')
# Satnab = Satnab.filter(col('BeneficiarySwiftCode')=='AYBKIRTHXXX')
# Satnab = Satnab.filter(~col('OrderOfSwiftCode').like("BMJIIR%"))
# Satnab = Satnab.filter(col('BeneficiaryCommentAcno')!='0300893205004')
# Satnab = Satnab.filter(col('TransactionType')!='i')
# Satnab= Satnab.withColumn("Bank", when(isnull(Satnab.Description),Satnab.OrderOfSwiftCode).otherwise(Satnab.Description))
# Satnab = Satnab.groupby(['OrderOfCommentAcno', 'Bank','BeneficiarySwiftCode']).agg(sum(col('Amount')).alias('amount'))
# Satnab = Satnab.withColumn('Branch', lit(None).cast(StringType()))
# Satnab = Satnab.withColumn('source', lit('satna').cast(StringType()))
# Satnab = Satnab.withColumn('type', lit('I').cast(StringType()))
# print(Satnab.head())
#
#
#
# ####                        Satna khorooji
#
#
# Satnab2 = Satna_DF.join (basicinfo_DF ,Satna_DF.OrderOfSwiftCode == basicinfo_DF.ValueCode,how='left')
# Satnab2 = Satnab2.filter(col('BeneficiarySwiftCode')=='AYBKIRTHXXX')
# Satnab2 = Satnab2.filter(col('OrderOfSwiftCode')!="BMJJIRTHXXX")
# Satnab2 = Satnab2.filter(col('BeneficiaryCommentAcno')!='0300893205004')
# Satnab2 = Satnab2.filter(col('TransactionType')!='i')
# Satnab2 = Satnab2.withColumn("Bank", when(isnull(Satnab2.Description),Satnab2.OrderOfSwiftCode).otherwise(Satnab2.Description))
# Satnab2 = Satnab2.groupby(['OrderOfCommentAcno', 'Bank','BeneficiarySwiftCode']).agg(sum(col('Amount')).alias('amount'))
# Satnab2 = Satnab2.withColumn('Branch', lit(None).cast(StringType()))
# Satnab2 = Satnab2.withColumn('source', lit('satna').cast(StringType()))
# Satnab2 = Satnab2.withColumn('type', lit('I').cast(StringType()))
# print(Satnab2.head())

TotalTxn_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/TotalTxn")
TotalTxn_DF.createOrReplaceTempView("TotalTxn")
TotalTxn_DF = TotalTxn_DF.withColumnRenamed("AcquireBankCode","Description")
TotalTxn_DF = TotalTxn_DF.withColumn('Branch', rtrim(TotalTxn_DF.Branch))
TotalTxn_DF = TotalTxn_DF.withColumn('Branch', ltrim(TotalTxn_DF.Branch))
TotalTxn_DF = TotalTxn_DF.withColumn('CardNo', rtrim(TotalTxn_DF.CardNo))
TotalTxn_DF = TotalTxn_DF.withColumn('CardNo', rtrim(TotalTxn_DF.CardNo))
TotalTxn_DF = TotalTxn_DF.withColumnRenamed("Branch","txnBranch")
TotalTxn_DF = TotalTxn_DF.withColumn('ProcessCode', substring(TotalTxn_DF.ProcessCode,0,2))
TotalTxn_DF = TotalTxn_DF.filter(col('SuccessOrFailure')=='S')
print(TotalTxn_DF.head())
TotalTxn_DF = TotalTxn_DF.filter((col('TerminalTypeCode')== '14') |
                                 (  ((col('TerminalTypeCode')== '59' )|(col('TerminalTypeCode')== '5')) &
                                    (col('LocalOrShetab')== 'si') &
                                            ((col('ProcessCode')== '17') |(col('ProcessCode')== '00')))
                                 )
print(TotalTxn_DF.head())
TotalCard = TotalTxn_DF.join (Card_DF ,TotalTxn_DF.CardNo == Card_DF.CARDNO,how='left')
print(TotalCard.head())
TotalCard = TotalCard.withColumnRenamed("Description","Bank")
TotalCard = TotalCard.withColumn('Custno', rtrim(TotalCard.CUSTNO))
TotalCard = TotalCard.withColumn('Custno', rtrim(TotalCard.Custno))
TotalCard = TotalCard.withColumn("Custno", when(((isnull(TotalCard.Custno))|(TotalCard.Custno=='')),TotalCard.ACNO).otherwise(TotalCard.Custno))
TotalCard = TotalCard.withColumn('Branch',concat(lit('0000'), col('BRANCH')))
TotalCard = TotalCard.withColumn('Branch', substring('Branch', -4, 4))
TotalCard = TotalCard.groupby(['Branch','Custno', 'Bank']).agg(sum(col('amount')).alias('amount'))
TotalCard = TotalCard.withColumn('source', lit('Shaparak').cast(StringType()))
TotalCard = TotalCard.withColumn('type', lit('O').cast(StringType()))
print(TotalCard.head())
Custinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Custinfo")
Custinfo_DF.createOrReplaceTempView("Custinfo")

SHPRTGS_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/SHPRTGS")
SHPRTGS_DF.createOrReplaceTempView("SHPRTGS")








print(CardDebit_DF.count())
print(CardGift_DF.count())
print(CardBon_DF.count())
print(Card_DF.count())




