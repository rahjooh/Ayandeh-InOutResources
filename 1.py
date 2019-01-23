import time ,pyspark ,socket ,jaydebeapi
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from os import listdir
from os.path import isfile, join

conf = pyspark.SparkConf()
spark = SparkSession.builder.getOrCreate()
print('\n\n\n\n\n\n\n\n\n\n\n\n')
lastdate= spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/LastDate")
yesterday =lastdate.head()[0]
today =lastdate.head()[1]
#
CardBon_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/CardBon")
CardBon_DF = CardBon_DF.withColumn('CARDNO', rtrim(CardBon_DF.CARDNO))
CardBon_DF = CardBon_DF.withColumn('CARDNO', ltrim(CardBon_DF.CARDNO))
CardBon_DF.createOrReplaceTempView("CardBon")
print(CardBon_DF.head())

CardDebit_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/CardDebit")
CardDebit_DF = CardDebit_DF.withColumn('CARDNO', rtrim(CardDebit_DF.CARDNO))
CardDebit_DF = CardDebit_DF.withColumn('CARDNO', ltrim(CardDebit_DF.CARDNO))
CardDebit_DF.createOrReplaceTempView("CardDebit")

CardGift_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/CardGift")
CardGift_DF = CardGift_DF.withColumn('CARDNO', rtrim(CardGift_DF.CARDNO))
CardGift_DF = CardGift_DF.withColumn('CARDNO', ltrim(CardGift_DF.CARDNO))
CardGift_DF = CardGift_DF.withColumn('CUSTNO', lit(None).cast(StringType()))
CardGift_DF.createOrReplaceTempView("CardGift")

Card_DF = CardDebit_DF.union(CardBon_DF).union(CardGift_DF)
Card_DF.repartition(6).createOrReplaceTempView("Card")



#
###                 paya voroodi
Incoming_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/Incomming")
Incoming_DF = Incoming_DF.withColumn('RecAccNo', ltrim(Incoming_DF.RecAccNo))
Incoming_DF = Incoming_DF.withColumn('RecAccNo', rtrim(Incoming_DF.RecAccNo))
Incoming_DF = Incoming_DF.withColumn('TrackID', ltrim(Incoming_DF.TrackID))
Incoming_DF = Incoming_DF.withColumn('TrackID', substring(Incoming_DF.TrackID,0,6))
Incoming_DF = Incoming_DF.withColumn('RecAccNo', substring('RecAccNo', -13, 13))
Incoming_DF = Incoming_DF.withColumnRenamed("RecAccNo","Acno")
Incoming_DF = Incoming_DF.withColumnRenamed("SenderBank","Bank")
'''Incoming_DF = Incoming_DF.filter(col('TrackID') == date )'''
Incoming_DF = Incoming_DF.filter(col('Acno') != '0300893205004')
Incoming_DF = Incoming_DF.groupby(['Acno', 'Bank']).agg(sum(col('Amount')).alias('amount'))
Incoming_DF = Incoming_DF.withColumn('Branch', lit(None).cast(StringType()))
Incoming_DF = Incoming_DF.withColumn('source', lit('paya').cast(StringType()))
Incoming_DF = Incoming_DF.withColumn('type', lit('I').cast(StringType()))
'''Incoming_DF = Incoming_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
Incoming_DF.createOrReplaceTempView("Incoming")
print('paya voroodi          ',Incoming_DF.take(1), (Incoming_DF.agg(sum("amount")).collect()))


actinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/Actinfo")
actinfo_DF = actinfo_DF.withColumn('ACNO', rtrim(actinfo_DF.ACNO))
actinfo_DF = actinfo_DF.withColumn('Acno', ltrim(actinfo_DF.ACNO))
actinfo_DF = actinfo_DF.withColumn('CUSTNO', rtrim(actinfo_DF.CUSTNO))
actinfo_DF = actinfo_DF.withColumn('Custno', ltrim(actinfo_DF.CUSTNO))
actinfo_DF.createOrReplaceTempView("actinfo")

####                paya khoroooji
Outgoing_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/Outgoing")
Outgoing_DF = Outgoing_DF.withColumn('SenderAccNo', ltrim(Outgoing_DF.SenderAccNo))
Outgoing_DF = Outgoing_DF.withColumn('SenderAccNo', rtrim(Outgoing_DF.SenderAccNo))
Outgoing_DF = Outgoing_DF.withColumn('TraceId', ltrim(Outgoing_DF.TraceId))
Outgoing_DF = Outgoing_DF.withColumn('TraceId', rtrim(Outgoing_DF.TraceId))
Outgoing_DF = Outgoing_DF.withColumn('SenderAccNo', substring('SenderAccNo', -13, 13))
Outgoing_DF = Outgoing_DF.withColumnRenamed("SenderAccNo","Acno")
Outgoing_DF = Outgoing_DF.withColumnRenamed("SenderBank","Bank")
'''Outgoing_DF = Outgoing_DF.filter(col('TraceId') == date )'''
Outgoing_DF = Outgoing_DF.filter(col('Acno') != '0300893205004')
Outgoing_DF = Outgoing_DF.groupby(['Acno', 'Bank']).agg(sum(col('Amount')).alias('amount'))
Outgoing_DF = Outgoing_DF.withColumn('Branch', lit(None).cast(StringType()))
Outgoing_DF = Outgoing_DF.withColumn('source', lit('paya').cast(StringType()))
Outgoing_DF = Outgoing_DF.withColumn('type', lit('I').cast(StringType()))
'''Outgoing_DF = Outgoing_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
Outgoing_DF.createOrReplaceTempView("Outgoing")
print('paya khoroooji          ',Outgoing_DF.take(1), (Outgoing_DF.agg(sum("amount")).collect()))


Chakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/Chakavak")
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

'''Chakavakin_DF = Chakavak_DF.filter(col('Hisdate') == date1 )'''
Chakavakin_DF = Chakavak_DF.filter(col('BenefBankCode') == 'AYBKIRTHXXX' )
Chakavakin_DF = Chakavakin_DF.filter(col('ChqFinalStat').like("%تا%") )
Chakavakin_DF = Chakavakin_DF.filter(~col('ChqType').like("%رمز%") )
Chakavakin_DF = Chakavakin_DF.filter(col('BenefMeliCode') != '10320894878')
Chakavakin_DF = Chakavakin_DF.groupby(['Bank', 'Branch','Acno']).agg(sum(col('Amount')).alias('amount'))
Chakavakin_DF = Chakavakin_DF.withColumn('source', lit('chakavak_non').cast(StringType()))
Chakavakin_DF = Chakavakin_DF.withColumn('type', lit('I').cast(StringType()))
'''Chakavakin_DF = Chakavakin_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
Chakavakin_DF.createOrReplaceTempView("Chakavakin")
print(' Chakavak non-ramzdar voroodi            ',Chakavakin_DF.take(1),  (Chakavakin_DF.agg(sum("amount")).collect()))







##                  Chakavak ramzdar voroodi

# '''Chakavak_DF = Chakavakin_DF.filter(col('Hisdate') == date1 )'''
Chakavakrin_DF = Chakavak_DF.filter(col('BenefBankCode') == 'AYBKIRTHXXX' )
Chakavakrin_DF = Chakavakrin_DF.filter(col('ChqFinalStat').like("%تا%") )
Chakavakrin_DF = Chakavakrin_DF.filter(col('ChqType').like("%رمز%") )
Chakavakrin_DF = Chakavakrin_DF.filter(col('BenefMeliCode') != '10320894878')
Chakavakrin_DF = Chakavakrin_DF.groupby(['Bank', 'Branch','Acno']).agg(sum(col('Amount')).alias('amount'))
Chakavakrin_DF = Chakavakrin_DF.withColumn('source', lit('chakavak_rz').cast(StringType()))
Chakavakrin_DF = Chakavakrin_DF.withColumn('type', lit('I').cast(StringType()))
'''Chakavakrin_DF = Chakavakrin_DF.withColumn('Hisdate', lit(date).cast(StringType()))'''
Chakavakrin_DF.createOrReplaceTempView("ChakavakinRamzdar")
# print('  Chakavak ramzdar voroodi            ', (Chakavakrin_DF.agg(sum("amount")).collect()))

#       Chakavak non-ramzdar khorooji

BllChakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/BLLCHAKAVAK")
BllChakavak_DF = BllChakavak_DF.withColumn('Acc', rtrim(BllChakavak_DF.Acc))
BllChakavak_DF = BllChakavak_DF.withColumn('Acc', ltrim(BllChakavak_DF.Acc))
BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', rtrim(BllChakavak_DF.OptionInfo1))
BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', ltrim(BllChakavak_DF.OptionInfo1))
BllChakavak_DF.createOrReplaceTempView("BllChakavak")
ch = Chakavak_DF.join (BllChakavak_DF , Chakavak_DF.OrderTrackNo == BllChakavak_DF.OptionInfo1, how='left')
ch = ch.withColumnRenamed("Acno","BenefSheba")
ch = ch.join (actinfo_DF ,ch.Acc == actinfo_DF.Acno,how='left')
'''ch = ch.filter(col('Hisdate') == date1 )'''
print(ch.head())
ch = ch.filter(col('BenefBankCode') != 'AYBKIRTHXXX')
ch = ch.filter(col('ChqFinalStat').like('%تا%'))
ch = ch.filter(~col('ChqType').like("%رمز%") )
ch = ch.filter(~col('BenefName').like("%بانک%") )
ch = ch.filter(~col('BenefName').like("%بانك%") )
ch = ch.filter(~col('BenefName').like("%خزانه%") )
ch = ch.filter(~col('BenefName').like("%مبادلات%") )
ch = ch.filter(col('OrderBankBranch') != '201')
ch = ch.withColumn('OrderBankBranch',concat(lit('0000'), col('OrderBankBranch')))
ch = ch.withColumn('OrderBankBranch', substring('OrderBankBranch', -4, 4))
ch = ch.filter(((col('Acc') != '030089325004')&(col('Amount') != ))|()) # ???????????????????????????????????????????????????????????????????????????????????????????????????????
ch = ch.withColumn("Acno", when(isnull(ch.Acc),ch.BenefMeliCode).otherwise(ch.Acc))
# ch = ch.groupby(['BenefBankCode', 'OrderBankBranch','Acno']).agg(sum(col('Amount')).alias('amount'))
# ch = ch.select("OrderTrackNo","Custno","Acno","BenefMeliCode","OrderBankBranch","BenefBankCode","TranAtmX","BenefName")
ch = ch.groupby(['Bank', 'Branch','Acno']).agg(sum(col('Amount')).alias('amount'))
ch = ch.withColumn('Branch', lit(None).cast(StringType()))
ch = ch.withColumn('source', lit('chakavak_non').cast(StringType()))
ch = ch.withColumn('type', lit('O').cast(StringType()))
print(ch.agg(count("amount")).collect())
# print(ch.take(2000))
print(' Chakavak non-ramzdar khorooji         ',ch.head(),  (ch.agg(sum("amount")).collect()))
exit(-1)


#       Chakavak ramzdar khorooji

BllChakavak_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/BLLCHAKAVAK")
BllChakavak_DF = BllChakavak_DF.withColumn('Acc', rtrim(BllChakavak_DF.Acc))
BllChakavak_DF = BllChakavak_DF.withColumn('Acc', ltrim(BllChakavak_DF.Acc))
BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', rtrim(BllChakavak_DF.OptionInfo1))
BllChakavak_DF = BllChakavak_DF.withColumn('OptionInfo1', ltrim(BllChakavak_DF.OptionInfo1))
BllChakavak_DF.createOrReplaceTempView("BllChakavak")
ch2 = Chakavak_DF.join (BllChakavak_DF , Chakavak_DF.OrderTrackNo == BllChakavak_DF.OptionInfo1, how='left')
ch2 = ch2.withColumnRenamed("Acno","BenefSheba")
ch2 = ch2.join (actinfo_DF ,ch2.Acc == actinfo_DF.Acno,how='left')
'''ch = ch.filter(col('Hisdate') == date1 )'''
ch2 = ch2.filter(col('BenefBankCode') != 'AYBKIRTHXXX')
ch2 = ch2.filter(col('ChqFinalStat').like('%تا%'))
ch2 = ch2.filter(col('ChqType').like("%رمز%") )
ch2 = ch2.filter(~col('BenefName').like("%بانک%") )
ch2 = ch2.filter(~col('BenefName').like("%بانك%") )
ch2 = ch2.filter(~col('BenefName').like("%خزانه%") )
ch2 = ch2.filter(~col('BenefName').like("%مبادلات%") )
ch2 = ch2.filter(col('OrderBankBranch') != '201')
ch2 = ch2.withColumn('OrderBankBranch',concat(lit('0000'), col('OrderBankBranch')))
ch2 = ch2.withColumn('OrderBankBranch', substring('OrderBankBranch', -4, 4))
### ch = ch.filter(col('Acc') != '030089325004') # ???????????????????????????????????????????????????????????????????????????????????????????????????????
ch2 = ch2.withColumn("Acno", when(isnull(ch2.Acc),ch2.BenefMeliCode).otherwise(ch2.Acc))
ch2 = ch2.select("OrderTrackNo","Custno","Acno","BenefMeliCode","OrderBankBranch","BenefBankCode","TranAtmX","BenefName")
ch2 = ch2.groupby(['BenefBankCode', 'OrderBankBranch','Acno']).agg(sum(col('TranAtmX')).alias('amount'))
ch2 = ch2.withColumn('Branch', lit(None).cast(StringType()))
ch2 = ch2.withColumn('source', lit('chakavak_rz').cast(StringType()))
ch2 = ch2.withColumn('type', lit('O').cast(StringType()))
print('Chakavak ramzdar khorooji       ',ch2.head(), (ch2.agg(sum("amount")).collect()))



basicinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/BasicInfo")
basicinfo_DF = basicinfo_DF.withColumn('ValueCode', rtrim(basicinfo_DF.ValueCode))
basicinfo_DF = basicinfo_DF.withColumn('ValueCode', ltrim(basicinfo_DF.ValueCode))
basicinfo_DF = basicinfo_DF.filter(col('FieldName')=='BIC_CODE')
basicinfo_DF = basicinfo_DF.filter(col('TableName')=='SATNA')
basicinfo_DF.createOrReplaceTempView("basicinfo")


Satna_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/Satna")
Satna_DF = Satna_DF.withColumn('OrderOfSwiftCode', rtrim(Satna_DF.OrderOfSwiftCode))
Satna_DF = Satna_DF.withColumn('OrderOfSwiftCode', ltrim(Satna_DF.OrderOfSwiftCode))
Satna_DF = Satna_DF.withColumn('BeneficiaryCommentShaba', rtrim(Satna_DF.BeneficiaryCommentShaba))
Satna_DF = Satna_DF.withColumn('BeneficiaryCommentShaba', ltrim(Satna_DF.BeneficiaryCommentShaba))
Satna_DF = Satna_DF.withColumn('BeneficiaryCommentShaba', substring('BeneficiaryCommentShaba', -13, 13))
Satna_DF.createOrReplaceTempView("Satna")


###              satna vorodi

Satnab = Satna_DF.join (basicinfo_DF ,Satna_DF.OrderOfSwiftCode == basicinfo_DF.ValueCode,how='left')
Satnab = Satnab.filter(col('BeneficiarySwiftCode')=='AYBKIRTHXXX')
Satnab = Satnab.filter(~col('OrderOfSwiftCode').like("BMJIIR%"))
Satnab = Satnab.filter(col('BeneficiaryCommentAcno')!='0300893205004')
Satnab = Satnab.filter(col('TransactionType')!='i')
Satnab= Satnab.withColumn("Bank", when(isnull(Satnab.Description),Satnab.OrderOfSwiftCode).otherwise(Satnab.Description))
Satnab = Satnab.groupby(['OrderOfCommentAcno', 'Bank','BeneficiarySwiftCode']).agg(sum(col('Amount')).alias('amount'))
Satnab = Satnab.withColumn('Branch', lit(None).cast(StringType()))
Satnab = Satnab.withColumn('source', lit('satna').cast(StringType()))
Satnab = Satnab.withColumn('type', lit('I').cast(StringType()))
print(' satna vorodi        ',Satnab.head(),(Satnab.agg(sum("amount")).collect()))



# ####                        Satna khorooji

Satnab2 = Satna_DF.join (basicinfo_DF ,Satna_DF.OrderOfSwiftCode == basicinfo_DF.ValueCode,how='left')
Satnab2 = Satnab2.filter(col('BeneficiarySwiftCode')=='AYBKIRTHXXX')
Satnab2 = Satnab2.filter(col('OrderOfSwiftCode')!="BMJJIRTHXXX")
Satnab2 = Satnab2.filter(col('BeneficiaryCommentAcno')!='0300893205004')
Satnab2 = Satnab2.filter(col('TransactionType')!='i')
Satnab2 = Satnab2.withColumn("Bank", when(isnull(Satnab2.Description),Satnab2.OrderOfSwiftCode).otherwise(Satnab2.Description))
Satnab2 = Satnab2.groupby(['OrderOfCommentAcno', 'Bank','BeneficiarySwiftCode']).agg(sum(col('Amount')).alias('amount'))
Satnab2 = Satnab2.withColumn('Branch', lit(None).cast(StringType()))
Satnab2 = Satnab2.withColumn('source', lit('satna').cast(StringType()))
Satnab2 = Satnab2.withColumn('type', lit('I').cast(StringType()))
print('Satna khorooji      ',Satnab2.head(),(Satnab2.agg(sum("amount")).collect()))



####                    Shaparak khoroooji

TotalTxn_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/TotalTxn")
TotalTxn_DF.createOrReplaceTempView("TotalTxn")
TotalTxn_DF = TotalTxn_DF.withColumnRenamed("AcquireBankCode","Description")
TotalTxn_DF = TotalTxn_DF.withColumn('Branch', rtrim(TotalTxn_DF.Branch))
TotalTxn_DF = TotalTxn_DF.withColumn('Branch', ltrim(TotalTxn_DF.Branch))
TotalTxn_DF = TotalTxn_DF.withColumn('CardNo', rtrim(TotalTxn_DF.CardNo))
TotalTxn_DF = TotalTxn_DF.withColumn('CardNo', ltrim(TotalTxn_DF.CardNo))
TotalTxn_DF = TotalTxn_DF.withColumnRenamed("Branch","txnBranch")
TotalTxn_DF = TotalTxn_DF.withColumn('ProcessCode', rtrim(TotalTxn_DF.ProcessCode))
TotalTxn_DF = TotalTxn_DF.withColumn('ProcessCode', ltrim(TotalTxn_DF.ProcessCode))
TotalTxn_DF = TotalTxn_DF.withColumn('SuccessOrFailure', rtrim(TotalTxn_DF.SuccessOrFailure))
TotalTxn_DF = TotalTxn_DF.withColumn('SuccessOrFailure', ltrim(TotalTxn_DF.SuccessOrFailure))
TotalTxn_DF = TotalTxn_DF.withColumn('TerminalTypeCode', rtrim(TotalTxn_DF.TerminalTypeCode))
TotalTxn_DF = TotalTxn_DF.withColumn('TerminalTypeCode', ltrim(TotalTxn_DF.TerminalTypeCode))
TotalTxn_DF = TotalTxn_DF.withColumn('LocalOrShetab', rtrim(TotalTxn_DF.LocalOrShetab))
TotalTxn_DF = TotalTxn_DF.withColumn('LocalOrShetab', ltrim(TotalTxn_DF.LocalOrShetab))
TotalTxn_DF = TotalTxn_DF.withColumn('ProcessCode', substring(TotalTxn_DF.ProcessCode,0,2))
TotalTxn_DF = TotalTxn_DF.filter(col('SuccessOrFailure')=='S')
TotalTxn_DF = TotalTxn_DF.filter((col('TerminalTypeCode')== '14') |
                                 (  ((col('TerminalTypeCode')== '59' )|(col('TerminalTypeCode')== '5')) &
                                    (col('LocalOrShetab')== 'si') &
                                            ((col('ProcessCode')== '17') |(col('ProcessCode')== '00')))
                                 )
TotalCard = TotalTxn_DF.join (Card_DF ,TotalTxn_DF.CardNo == Card_DF.CARDNO,how='left')
TotalCard = TotalCard.withColumnRenamed("Description","Bank")
TotalCard = TotalCard.withColumn('Custno', rtrim(TotalCard.CUSTNO))
TotalCard = TotalCard.withColumn('Custno', rtrim(TotalCard.Custno))
TotalCard = TotalCard.withColumn("Custno", when(((isnull(TotalCard.Custno))|(TotalCard.Custno=='')),TotalCard.ACNO).otherwise(TotalCard.Custno))
TotalCard = TotalCard.withColumn('Branch',concat(lit('0000'), col('BRANCH')))
TotalCard = TotalCard.withColumn('Branch', substring('Branch', -4, 4))
TotalCard = TotalCard.groupby(['Branch','Custno', 'Bank']).agg(sum(col('amount')).alias('amount'))
TotalCard = TotalCard.withColumn('source', lit('Shaparak').cast(StringType()))
TotalCard = TotalCard.withColumn('type', lit('O').cast(StringType()))
print('Shaparak khoroooji       ',TotalCard.head(),(TotalCard.agg(sum("amount")).collect()))

# Custinfo_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Custinfo")
# Custinfo_DF.createOrReplaceTempView("Custinfo")


###                 shaparak voroodi
SHPRTGS_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/SHPRTGS")
SHPRTGS_DF = SHPRTGS_DF.withColumn('Iban', rtrim(SHPRTGS_DF.IBAN))
SHPRTGS_DF = SHPRTGS_DF.withColumn('Iban', rtrim(SHPRTGS_DF.Iban))
SHPRTGS_DF = SHPRTGS_DF.withColumn('acno', substring('Iban', -13, 13))
SHPRTGS_DF = SHPRTGS_DF.withColumnRenamed("PspCode","Bank")
SHPRTGS_DF = SHPRTGS_DF.groupby('acno',"Bank").agg(sum(col('AmountShaparak')).alias('amount'))
SHPRTGS_DF = SHPRTGS_DF.withColumn('Branch', lit(None).cast(StringType()))
SHPRTGS_DF = SHPRTGS_DF.withColumn('source', lit('Shaprak').cast(StringType()))
SHPRTGS_DF = SHPRTGS_DF.withColumn('type', lit('I').cast(StringType()))
SHPRTGS_DF.createOrReplaceTempView("SHPRTGS")
print('shaparak voroodi      ',SHPRTGS_DF.head(), (SHPRTGS_DF.agg(sum("amount")).collect()))




TotalTxn2_DF = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/"+yesterday+"/TotalTxn")
TotalTxn2_DF = TotalTxn2_DF.withColumn('CardNo', rtrim(TotalTxn2_DF.CardNo))
TotalTxn2_DF = TotalTxn2_DF.withColumn('CardNo1', rtrim(TotalTxn2_DF.CardNo))
TotalTxn2_DF = TotalTxn2_DF.withColumn('pan6', substring(TotalTxn2_DF.CardNo,0,6))
TotalTxn2_DF = TotalTxn2_DF.withColumn('ProcessCode', rtrim(TotalTxn2_DF.ProcessCode))
TotalTxn2_DF = TotalTxn2_DF.withColumn('ProcessCode', rtrim(TotalTxn2_DF.ProcessCode))
TotalTxn2_DF = TotalTxn2_DF.withColumn('ProcessCode', substring(TotalTxn2_DF.ProcessCode,0,2))
TotalTxn2_DF = TotalTxn2_DF.withColumn('DestPan', rtrim(TotalTxn2_DF.DestPan))
TotalTxn2_DF = TotalTxn2_DF.withColumn('DestPan', ltrim(TotalTxn2_DF.DestPan))
TotalTxn2_DF = TotalTxn2_DF.withColumn('Dpan6', substring(TotalTxn2_DF.CardNo,0,6))
TotalTxn2_DF = TotalTxn2_DF.withColumn('SuccessOrFailure', rtrim(TotalTxn2_DF.SuccessOrFailure))
TotalTxn2_DF = TotalTxn2_DF.withColumn('SuccessOrFailure', ltrim(TotalTxn2_DF.SuccessOrFailure))
TotalTxn2_DF = TotalTxn2_DF.withColumn('TerminalTypeCode', rtrim(TotalTxn2_DF.TerminalTypeCode))
TotalTxn2_DF = TotalTxn2_DF.withColumn('TerminalTypeCode', ltrim(TotalTxn2_DF.TerminalTypeCode))
TotalTxn2_DF = TotalTxn2_DF.withColumn('LocalOrShetab', rtrim(TotalTxn2_DF.LocalOrShetab))
TotalTxn2_DF = TotalTxn2_DF.withColumn('LocalOrShetab', ltrim(TotalTxn2_DF.LocalOrShetab))
TotalTxn2_DF = TotalTxn2_DF.withColumnRenamed("Branch","Branch1")

##                          shetab voroodi

TotalTxn2in_DF = TotalTxn2_DF.filter(col('SuccessOrFailure')=='S')
TotalTxn2in_DF = TotalTxn2in_DF.filter(col('LocalOrShetab')=='Lo')
TotalTxn2in_DF = TotalTxn2in_DF.filter(col('Amount')> 0)
TotalTxn2in_DF = TotalTxn2in_DF.filter(((TotalTxn2in_DF.ProcessCode =='47')&(TotalTxn2in_DF.pan6 =='636214'))
                                   |((TotalTxn2in_DF.ProcessCode =='19')&(TotalTxn2in_DF.pan6 !='636214')))
TotalCard2 = TotalTxn2in_DF.join (Card_DF ,TotalTxn2in_DF.CardNo1 == Card_DF.CARDNO,how='left')
TotalCard2 = TotalCard2.withColumn("Branch", when((TotalCard2.pan6 != '636214'),TotalCard2.Branch1).otherwise(TotalCard2.BRANCH))
TotalCard2 = TotalCard2.withColumn("Acno", when(isnull(TotalCard2.CUSTNO),TotalCard2.CardNo1).otherwise(TotalCard2.ACNO))
TotalCard2 = TotalCard2.withColumn("Bank", when((TotalCard2.ProcessCode == '19'),TotalCard2.pan6).otherwise(TotalCard2.Dpan6))
TotalCard2 = TotalCard2.groupby(['Branch','Acno', 'Bank']).agg(sum(col('Amount')).alias('amount'))
TotalCard2 = TotalCard2.withColumn('Branch',concat(lit('0000'), col('BRANCH')))
TotalCard2 = TotalCard2.withColumn('Branch', substring('Branch', -4, 4))
TotalCard2 = TotalCard2.withColumn('source', lit('Shetab').cast(StringType()))
TotalCard2 = TotalCard2.withColumn('type', lit('I').cast(StringType()))
print('shetab voroodi    ',TotalCard2.head(), (TotalCard2.agg(sum("amount")).collect()))

##                          shetab khorooji
TotalTxn2out_DF = TotalTxn2_DF.filter(col('SuccessOrFailure')=='S')
TotalTxn2out_DF = TotalTxn2out_DF.filter(col('LocalOrShetab')!='Lo')
TotalTxn2out_DF = TotalTxn2out_DF.filter(col('Amount')> 0)
TotalTxn2out_DF = TotalTxn2out_DF.filter(col('pan6')== '636214')
TotalTxn2out_DF = TotalTxn2out_DF.filter(~col('ProcessCode').isin({"00", "60", "47", "33"}))
TotalTxn2out_DF = TotalTxn2out_DF.filter((col('ProcessCode').isin({"46", "01"}))
                                         | ( ((col('ProcessCode')=='17')&(~col('TerminalTypeCode').isin({"14", "59", "05"}))  )))
TotalCard3 = TotalTxn2out_DF.join (Card_DF ,TotalTxn2out_DF.CardNo1 == Card_DF.CARDNO,how='left')
TotalCard3 = TotalCard3.withColumn("Acno", when(isnull(TotalCard3.CUSTNO),TotalCard3.CardNo1).otherwise(TotalCard3.ACNO))
TotalCard3 = TotalCard3.withColumn("Bank", when((TotalCard3.ProcessCode == '46'),TotalCard3.Dpan6).otherwise(TotalCard3.AcquireBankCode))
TotalCard3 = TotalCard3.groupby(['Branch','Acno', 'Bank']).agg(sum(col('Amount')).alias('amount'))
TotalCard3 = TotalCard3.withColumn('source', lit('Shetab').cast(StringType()))
TotalCard3 = TotalCard3.withColumn('type', lit('O').cast(StringType()))
print('shetab khorooji  ' ,TotalCard3.head(), (TotalCard3.agg(sum("amount")).collect()))


