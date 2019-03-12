import findspark
findspark.init()
from pyspark.sql import SparkSession

def get_file_row_count():
    spark = SparkSession.builder.appName("Anything").getOrCreate()
    # df = spark.read.json('D:/Users/URaut/Pictures/Files_for_Infosys/')
    # tra_list= df.select('notifications.POSLog.Transaction.TransactionID').collect()
    df = spark.read.json('D:/Users/URaut/Videos/ETL-master/people')
    tra_list= df.select('salary').collect()
    tra_list
    # tra_list[0].TransactionID
    return len(tra_list)
