def check_data_quality(spark, df, table_name, column_to_retrieve_after_null_check, 
                        columns_to_check_for_null, check_type):
    list_of_col_data = []
    if(check_type == "is null"):
        for chk in range (0, len(columns_to_check_for_null)):
            null_check_df = spark.sql(
                "select {0} from {1} where {2} is null or {2} == 'null'".format(column_to_retrieve_after_null_check,
                table_name, columns_to_check_for_null[chk])
                )
            list_of_col_data.append(null_check_df)
    else:
        for chk in range (0, len(columns_to_check_for_null)):
            null_check_df = spark.sql(
                "select {0} from {1} where {2} is not null or {2} != 'null'".format(column_to_retrieve_after_null_check,
                table_name, columns_to_check_for_null[chk])
                )
            list_of_col_data.append(null_check_df)
    return list_of_col_data
