import csv

def generate_report(data, table_name):
    print("Difference between the number of rows in Hive table and the number of file records = {0}".format(data[0]))
    print(data[1])
    difference = data[0]
    comments = data[1]
    hive_tbl_row_count = data[2]
    input_file_row_count = data[3]
    aggregate_result = data[4] 
    null_check_data = data[5]
    comments_after_null_chk = data[6]
    comp_result_list = data[7]

    with open("D:/Users/URaut/Desktop/utilities/reports/Agg_Results.csv", "w") as csvfile:
        fieldnames = ['Table', 'Column', 'Agg_Func', 'Agg_Val']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()   
        for result in aggregate_result:
            for col_name, agg_key_val in result.items():
                for agg_opn_name, agg_val in agg_key_val.items():
                    print("{0} columns's {1} value is {2}".format(col_name, agg_opn_name, agg_val))
                    writer.writerow({'Table': table_name, 'Column': col_name, 'Agg_Func': agg_opn_name, 'Agg_Val': agg_val})

    with open("D:/Users/URaut/Desktop/utilities/reports/Row_count_val_Results.csv", "w") as csvfile:
        fieldnames = ['Number of records from POS', 'Number of transactions loaded into stage', 'Difference', 'Comments']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()   
        writer.writerow({'Number of records from POS': input_file_row_count, 
                        'Number of transactions loaded into stage': hive_tbl_row_count,
                        'Difference': difference, 'Comments': comments})

    with open("D:/Users/URaut/Desktop/utilities/reports/Null_check_Results.csv", "w") as csvfile:
        fieldnames = ['Table', 'Comments', 'TransactionIDs']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        writer.writerow({'Table': table_name,  'Comments': comments_after_null_chk})   
        for data in null_check_data:
            writer.writerow({'TransactionIDs': data})

    with open("D:/Users/URaut/Desktop/utilities/reports/Compare_Results.csv", "w") as csvfile:
        fieldnames = ['TransactionID','Table_data_SequenceNumber', 'File_data_SequenceNumber']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        for result in comp_result_list:
            list_of_keys = []
            for key, val in result.items():
                list_of_keys.append(key)            
            writer.writerow({'TransactionID': result.get(list_of_keys[0]) , 'Table_data_SequenceNumber': result.get(list_of_keys[1]), 'File_data_SequenceNumber': result.get(list_of_keys[2])})

    
    
    
