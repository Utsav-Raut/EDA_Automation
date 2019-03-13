def check_data_quality(spark, df, table_name, column_to_retrieve_after_null_check, 
                        columns_to_check_for_null, check_type):
    
    list_of_filtered_cols_after_null_chk = []
    if(check_type == "is null"):
        for chk in range (0, len(columns_to_check_for_null)):
            list_of_col_data = []
            filter_cols_after_null_chk = {}
            null_check_df = spark.sql(
                "select {0} from {1} where {2} is null or {2} == 'null'".format(column_to_retrieve_after_null_check,
                table_name, columns_to_check_for_null[chk])
                )
            for i in range (0, null_check_df.count()):
                list_of_col_data.append(null_check_df.collect()[i].__getitem__(column_to_retrieve_after_null_check))
            list_of_col_data.insert(0, columns_to_check_for_null[chk])
            filter_cols_after_null_chk[column_to_retrieve_after_null_check] = list_of_col_data
            list_of_filtered_cols_after_null_chk.append(filter_cols_after_null_chk)
    else:
        for chk in range (0, len(columns_to_check_for_null)):
            list_of_col_data = []
            filter_cols_after_null_chk = {}
            null_check_df = spark.sql(
                "select {0} from {1} where {2} is not null or {2} != 'null'".format(column_to_retrieve_after_null_check,
                table_name, columns_to_check_for_null[chk])
                )
            for i in range (0, null_check_df.count()):
                list_of_col_data.append(null_check_df.collect()[i].__getitem__(column_to_retrieve_after_null_check))
            list_of_col_data.insert(0, columns_to_check_for_null[chk])
            filter_cols_after_null_chk[column_to_retrieve_after_null_check] = list_of_col_data
            list_of_filtered_cols_after_null_chk.append(filter_cols_after_null_chk)

    return list_of_filtered_cols_after_null_chk
