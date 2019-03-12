def get_agg_result(df, column_with_agg_func):
    
    agg_result = []
    for list_of_data in column_with_agg_func:
        for i in range(1,len(list_of_data)):
        #     agg_result.append(df.agg({list_of_data[0]: format(list_of_data[i])}))
                result = df.agg({list_of_data[0]: format(list_of_data[i])})
                agg_dict_with_value = {}
                dict_of_col_name_with_agg_values = {}
                agg_dict_with_value[list_of_data[i]] = result.head()[0]
                dict_of_col_name_with_agg_values[list_of_data[0]] = agg_dict_with_value
                agg_result.append(dict_of_col_name_with_agg_values)
    return agg_result
