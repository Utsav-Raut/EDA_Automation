from pyspark.sql import functions as f
def check_data_quality(spark, df, table_name, column_to_retrieve_after_null_check, 
                        columns_to_check_for_null, check_type):
    
    list_of_filtered_cols_after_null_chk = []
    if(check_type == "is null"):
        for chk in range (0, len(columns_to_check_for_null)):
            list_of_null_data = []
            # filter_cols_after_null_chk = {}
            comment = ""
            null_check_df = spark.sql(
                "select {0} from {1} lateral view explode({2}) dummy as dummy_tbl where dummy_tbl is null or dummy_tbl == 'null'".format(column_to_retrieve_after_null_check,
                table_name, columns_to_check_for_null[chk])
                )
            if null_check_df.count() > 0:
                null_rows = null_check_df.collect()
                comment = "Here are the null values which are in the table"
                for rows in null_rows:
                    for item in rows.asDict().values():
                        list_of_null_data.append(item[0])
            else:
                list_of_null_data
                comment = "There is no null data present in the table"

            # for i in range (0, null_check_df.count()):
            #     list_of_col_data.append(null_check_df.collect()[i].__getitem__(column_to_retrieve_after_null_check))
            # list_of_col_data.insert(0, columns_to_check_for_null[chk])
            # filter_cols_after_null_chk[column_to_retrieve_after_null_check] = list_of_col_data
            # list_of_filtered_cols_after_null_chk.append(filter_cols_after_null_chk)
    else:
        for chk in range (0, len(columns_to_check_for_null)):
            list_of_null_data = []
            comment = ""
            not_null_df = spark.sql("select {0} from {1} lateral view explode({2}) dummy as dummy_tbl where dummy_tbl is not null or dummy_tbl != 'null'".format(column_to_retrieve_after_null_check,
                                table_name, columns_to_check_for_null[chk]))
            not_null_df.show()
            if not_null_df.count() > 0:
                null_rows = not_null_df.collect()
                comment = "These are the not-null values present"
                for rows in null_rows:
                    for item in rows.asDict().values():
                        list_of_null_data.append(item[0])
            else:
                list_of_null_data
                comment = "There is/are null data present in the table"
    #         for i in range (0, null_check_df.count()):
    #             list_of_col_data.append(null_check_df.collect()[i].__getitem__(column_to_retrieve_after_null_check))
    #         print(list_of_col_data)
    #         list_of_col_data.insert(0, columns_to_check_for_null[chk])
    #         filter_cols_after_null_chk[column_to_retrieve_after_null_check] = list_of_col_data
    #         list_of_filtered_cols_after_null_chk.append(filter_cols_after_null_chk)

    return list_of_null_data, comment
