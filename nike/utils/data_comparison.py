import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions
from utils.compare import get_file_details, get_hive_tbl_details
from pyspark.sql.functions import explode, col

def compare_data(spark, df, fields_to_compare):
    file_df = get_file_details()
    file_df.show()
    print("***********")
    comp_result_list = []
    dict_with_val = {}
    for list_of_fields in range(1, len(fields_to_compare)): 
        df2 = file_df.join(df, [file_df[fields_to_compare[0]] == df[fields_to_compare[0]]])
        df2.show()        
        print(fields_to_compare[list_of_fields])
        comp_result=df2.filter(df[fields_to_compare[1]]!=file_df[fields_to_compare[1]]).select(df[fields_to_compare[0]].alias("{}".format(fields_to_compare[0])),file_df[fields_to_compare[1]].alias("{}".format(fields_to_compare[1])),df[fields_to_compare[1]].alias("table_data"))
        comp_result.show()
        
        comp_result_len = len(comp_result.columns)
        print(comp_result.collect()[0])
        dict_with_val = {}
        print(comp_result.count())
        for i in range(0, comp_result.count()):
            dict_with_val = {}
            for j in range(0, comp_result_len):
                dict_with_val[fields_to_compare[j]] = comp_result.collect()[i][j]
            comp_result_list.append(dict_with_val)
    return comp_result_list
