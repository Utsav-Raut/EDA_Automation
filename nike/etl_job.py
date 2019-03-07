from dependencies.spark import start_spark
from utils.aggregate_ops import get_agg_result
from utils.row_count_validation import row_count_val
from pyspark.sql.types import DoubleType, IntegerType
from utils.file_ops import get_files_details
def main():
    """Main ETL script definition.
    :return: None
    """
    # Start spark application and get spark session, logger and config
    spark, log, config = start_spark(
        app_name = 'nike_etl_job',
        files = ['configs/etl_config.json']
    )
    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # Execute ETL pipeline
    data = extract_data(spark, config['db_name'], config['table_name'])
    data_transformed = transformed_data(data, spark, config['column_with_agg_func'], 
                        config['table_name'], config['field_name'])
    load_data(data_transformed)

    # log the success and terminate the spark session
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def extract_data(spark, db_name, table_name):
    """Load data from Hive table"""
    # spark.sql("show databases").show()
    spark.sql("use {0}".format(db_name)) # This is a hive db
    df = spark.sql(
        "select * from {0}".format(table_name)
    )
    # df.show()
    return df

def transformed_data(df, spark, column_with_agg_func, table_name, field_name):
    "Transform the original data set"
    # df.coalesce(1).write.csv("D:/Users/URaut/Pictures/output1.csv")
    # df_transformed = df.select("Firstname")
    file_data_count = get_files_details()
    tbl_data_count = row_count_val(spark, df, field_name, table_name)
    records_diff = tbl_data_count - file_data_count
    if records_diff == 0:
        print('Table and files have same number of records')
    elif records_diff > 0:
        print('Table have more records than the files')
    else:
        print('Files have more data than table')
    df_transformed = df
    return df_transformed

def load_data(df):
    df.show()

# Entry point for PySpark ETL application
if __name__ == '__main__':
    main()
