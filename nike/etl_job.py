from dependencies.spark import start_spark
from utils.aggregate_operations import get_agg_result
from pyspark.sql.types import DoubleType, IntegerType
from utils.row_count_validation import get_row_count_diff
from utils.data_quality import check_data_quality
from utils.get_report import generate_report
from utils.data_comparison import compare_data
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
    data_transformed = transformed_data(data, spark, config['table_name'], config['file_path'], 
                        config['field_name'], config['column_with_agg_func'],
                        config['fields_to_compare'], config['path_to_report_directory'], 
                        config['column_to_retrieve_after_null_check'], config['columns_to_check_for_null'], 
                        config['check_type'])
    load_data(data_transformed, config['table_name'])

    # log the success and terminate the spark session
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def extract_data(spark, db_name, table_name):
    """Load data from Hive table"""
    spark.sql("use {0}".format(db_name)) # This is a hive db
    df = spark.sql(
        "select * from {0}".format(table_name)
    )
    return df

def transformed_data(df, spark, table_name, file_path, field_name, 
                    column_with_agg_func, fields_to_compare, path_to_report_directory, column_to_retrieve_after_null_check,
                    columns_to_check_for_null, check_type):
    "Transform the original data set"
    row_count_diff, result, hive_tbl_row_count, input_file_row_count = get_row_count_diff(spark, df, field_name, table_name, file_path)
    agg_output = get_agg_result(df, spark, table_name, column_with_agg_func)
    comp_data = compare_data(spark, df, fields_to_compare)
    list_of_rows_with_null_data, comments = check_data_quality(spark, df, table_name, column_to_retrieve_after_null_check, columns_to_check_for_null, check_type)
    for x in list_of_rows_with_null_data:
        print(x)
    print(comments)
    df_transformed = [row_count_diff, result, hive_tbl_row_count, input_file_row_count, agg_output, 
                        list_of_rows_with_null_data, comments, comp_data]
    # df_transformed = [row_count_diff, result, hive_tbl_row_count, input_file_row_count, agg_output, comp_data]
    # df_transformed = df
    return df_transformed

def load_data(df_transformed, table_name):
    print('Hello')
    generate_report(df_transformed, table_name)

# Entry point for PySpark ETL application
if __name__ == '__main__':
    main()
