from utils import hive_table_operations, file_operations

def get_row_count_diff(spark, df, field_name, table_name):
    hive_tbl_row_count = hive_table_operations.get_hive_tbl_row_count(spark, df, field_name, table_name)
    input_file_row_count = file_operations.get_file_row_count()

    row_count_diff = hive_tbl_row_count - input_file_row_count

    if row_count_diff == 0:
        output = 'Hive Table and files have equal number of records'
    elif row_count_diff > 0:
        output = 'Hive Table has more records than the files'
    else:
        output = 'Files have more data than Hive table'

    return row_count_diff, output, hive_tbl_row_count, input_file_row_count
