import findspark
findspark.init()
from pyspark.sql import SparkSession

def get_files_details():
    spark = SparkSession.builder.appName("Anything").getOrCreate()
    # df = spark.read.json('D:/Users/URaut/Pictures/Files_for_Infosys/')
    # tra_list= df.select('notifications.POSLog.Transaction.TransactionID').collect()
    df = spark.read.json('D:/Users/URaut/Videos/ETL-master/people')
    tra_list= df.select('salary').collect()
    tra_list
    # tra_list[0].TransactionID
    print(len(tra_list))
    return len(tra_list)
# get_files_details()
