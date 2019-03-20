import findspark
findspark.init()
from pyspark.sql import SparkSession

def get_file_details():
    spark = SparkSession.builder.appName('Read File').getOrCreate()
    df = spark.read.json('D:/Users/URaut/Desktop/utilities/master_data.json')
    return df

def get_hive_tbl_details():
    spark = SparkSession.builder.appName('Read Table').enableHiveSupport().getOrCreate()
    spark.sql('use userdb')
    df = spark.sql('select * from demo_tran')
    return df

def compare_data():
    df2 = get_hive_tbl_details()
    df1 = get_file_details()
    # df1.show()
    # df2.show()

    # join_df = df1.join(df2,[df1.notifications.POSLog.Transaction.TransactionID == df2.notifications.POSLog.Transaction.TransactionID])
    # res_df = join_df.filter(df1.notifications.POSLog.Transaction.SequenceNumber != df2.notifications.POSLog.Transaction.SequenceNumber).select(df1.notifications.POSLog.Transaction.TransactionID.alias('TransactionID'),df1.notifications.POSLog.Transaction.SequenceNumber.alias('json_TxnAmt'),df2.notifications.POSLog.Transaction.SequenceNumber.alias('table_TxnAmt'))
    # res_df.show()
    # print(join_df)
    # join_df.show()

compare_data()
