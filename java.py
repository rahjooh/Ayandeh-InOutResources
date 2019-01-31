# SparkSession spark = SparkSession.builder().master("yarn").appName("Application").enableHiveSupport().getOrCreate();
# Dataset<Row> ds = spark.read().parquet(filename);