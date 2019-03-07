def row_count_val(spark, df, field_name, table_name):
    tbl_row_count_df = spark.sql(
        "select count({0}) from {1}".format(field_name, table_name)
    )
    # tbl_row_count_df.show()
    tbl_row_count = tbl_row_count_df.head()[0]
    print(tbl_row_count)
    return tbl_row_count
