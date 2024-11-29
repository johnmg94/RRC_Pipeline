import os
import pandas as pd
import re
import sqlalchemy
from sqlalchemy import create_engine

file_path = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV'

files = os.listdir(file_path)

def bulk_update():
    for item in files:
        print(item)
        # Construct full path to the file
        file_path_edit = os.path.join(file_path, item)
        
        # Read the file content
        with open(file_path_edit, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Replace "}" with ","

        new_text = content.replace("}", ",")
        
        # Write the modified content back to the file
        with open(file_path_edit, "w", encoding="utf-8") as f:
            f.write(new_text)

# -------------------------------------
# OG_SUMMARY_ONSHORE_LEASE_DATA_TABLE
# OG_WELL_COMPLETION_DATA_TABLE
# OG_COUNTY_CYCLE_DATA_TABLE
# OG_SUMMARY_MASTER_LARGE_DATA_TABLE
# OG_REGULATORY_LEASE_DW_DATA_TABLE
# OG_OPERATOR_DW_DATA_TABLE
# OG_OPERATOR_CYCLE_DATA_TABLE
# OG_LEASE_CYCLE_DISP_DATA_TABLE
# OG_LEASE_CYCLE_DATA_TABLE
# OG_FIELD_DW_DATA_TABLE
# OG_FIELD_CYCLE_DATA_TABLE
# OG_DISTRICT_CYCLE_DATA_TABLE
# GP_DISTRICT_DATA_TABLE
# GP_COUNTY_DATA_TABLE
# OG_COUNTY_LEASE_CYCLE_DATA_TABLE

# def to_pd():
    # file_in = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\CSV\OG_COUNTY_LEASE_CYCLE_DATA_TABLE.csv'
    # try:
    #     with open(file_in 'r') as f:
    #         df = pd.read_csv()

            
def stream_data():
    try:
        engine = create_engine('mysql+pymysql://mdr_usr_db_admin:6@BSRVDr&Q8xFKnfU4Cm@localhost:3306')
        if (engine):
            print(type(engine))
    except Exception as e:
        print("Engine connection failed", (e))
    print("hello")


    # with engine.connect() as connection:

x = stream_data()
# def length_of_csv():
#     folder_out = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\CSV\OG_COUNTY_LEASE_CYCLE_DATA_TABLE\OGCLCDT\0.csv'
#     file_len = 0
#     with open (folder_out, 'r') as f:
        
def csv_partition():
    file_in = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\CSV\OG_COUNTY_LEASE_CYCLE_DATA_TABLE.csv'
    folder_out = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\CSV\OG_COUNTY_LEASE_CYCLE_DATA_TABLE\OGCLCDT'
    count = 0
    name_count = 0
    temp_arr = []
    file_len = 0
    # with open(file_in, 'r') as k:
    #     for line_item in k:
    #         file_len += 1
    #     print(file_len)
    #     file_in.close()
    with open(file_in, 'r') as f:
        for line in f:
            # print(line)
            temp_arr.append(line)
            count += 1
            if count % 10243124 == 0:
                file_path = os.path.join(folder_out, f"{name_count}.csv")
                try:
                    with open(file_path, "w") as g:
                        for item in temp_arr:
                            g.write(item)
                        temp_arr = []
                        name_count += 1
                        print("File created at: ", str(file_path))
                except Exception as e:
                    print(f"Error writing to file {file_path}: {e}")

        if temp_arr:
            name_count += 1
            file_path = os.path.join(folder_out, f"{name_count}.csv")
            try:
                with open(file_path, "w") as g:
                    for item in temp_arr:
                        g.write(item)
                    print("Final file created at:", file_path)
            except Exception as e:
                print(f"Error writing to file {file_path}: {e}")
        print("Partitioning complete.")

# x = csv_partition()

# Matching for more than one '}' is INCORRECT***
def bug_1():
    pattern = r"}"
    output_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\CSV\GP_DISTRICT_DATA_TABLE.csv'
    input_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\GP_DISTRICT_DATA_TABLE.dsv'
    # count = 0
    with open(input_file, "r", encoding="utf-8") as infile:
        with open(output_file, "a", encoding="utf-8") as f:
            for line in infile:
                # if count < 10:
                comma_deleted_text = re.sub(r',', " ", line)
                comma_added_text = re.sub(pattern, ",", comma_deleted_text) 
                # print(comma_added_text)
                f.write(comma_added_text)
                # else:
                #     break
                # count += 1
                # matched_line = None
# x = bug_1()

# View 10 lines of .dsv and out to \BUG folder
def single_update_read():
    # pattern = r"}+"
    output_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\BUG\OG_COUNTY_CYCLE_DATA_TABLE.dsv'
    input_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\OG_COUNTY_CYCLE_DATA_TABLE.dsv'
    count = 0
    with open(input_file, "r", encoding="utf-8") as infile:
        with open(output_file, "a", encoding="utf-8") as f:
            for line in infile:
                # matched_line = re.sub(pattern, ",", line)
                if count < 1000:
                    f.write(line)
                    print(line)
                else:
                    break
                count += 1
                # matched_line = None

# x = single_update_read()

# Pandas to_csv doesn't accept regex as input
def single_file():
    pattern = r'}+'
    input_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\OG_COUNTY_CYCLE_DATA_TABLE.dsv'
    output_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\OG_COUNTY_CYCLE_DATA_TABLE.csv'

    chunk_size = 100000
    delimiter = '}'

    for chunk in pd.read_csv(input_file, delimiter=delimiter, chunksize=chunk_size, engine='python'):
        chunk.to_csv(output_file, mode='a',index=False, header=False)

def chunking():
    input_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\OG_COUNTY_LEASE_CYCLE_DATA.dsv'
    output_file = r'C:\Users\johnm\Code\Heta\Heta\BackEnd\assets\documents_20241122\PDQ_DSV\OG_COUNTY_LEASE_CYCLE_DATA.csv'

    chunk_size = 100000
    delimiter = "|"

    for chunk in pd.read_csv(input_file, delimiter=delimiter, chunksize=chunk_size):
        chunk.to_csv(output_file, mode='a',index=False, header=False)
