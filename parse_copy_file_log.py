"""
        Usage spark-submit script_name.py logfile_path
"""


import pandas as pd
from pandas import DataFrame
import sys
import os
import datetime
import subprocess
import numpy as np


def parse_log(log_file_path: str):
    """

    :param log_file_path: the path of the log file
    :return: a dataframe with the information for the analysis
    """
    log_file_path = os.path.abspath(log_file_path)
    column_names = ['Timestamp', 'Status', 'Process_stage', 'Contents']
    df = pd.DataFrame({}, columns=column_names)
    f = open(log_file_path, 'r')
    lines = f.readlines()
    f.close()
    time_list = []
    status_list = []
    process_stage_list = []
    contents_list = []
    for line in lines:
        line_split = line.split('-')
        time_list.append(line_split[0])
        status_list.append(line_split[1])
        process_stage_list.append(line_split[2])
        contents_list.append(''.join(line_split[3:]).strip())
    df['Timestamp'] = time_list
    status_list_updated = list(map(lambda x: x if x.isspace() else 'OK', status_list))
    df['Status'] = list(map(lambda x:x if not x.isspace() else 'No status', status_list_updated))
    df['Process_stage'] = process_stage_list
    df['Contents'] = contents_list
    return df


def modify_data_frame(data_df: DataFrame):
    """

    :param data_df: dataframe brut provenant des logs CFT parsés par la fonction parse_log
    :return: Dataframe pandas avec l'horodatage mis à jour et la duréé de chaque phase
    """
    timestamp_list = list(data_df['Timestamp'])
    date_list = []
    time_list = []
    format_type = "%Y%m%d%H%M%S"
    for date_string in timestamp_list:
        dt = datetime.datetime.strptime(date_string.rstrip(), format_type)
        date_list.append(str(dt.date()))
        time_list.append(str(dt.time()))
    new_df = data_df.copy()
    new_df['Production_date'] = date_list
    new_df['Production_time'] = time_list
    new_df = new_df.drop(columns='Timestamp')
    new_df.iloc[0, 2] = new_df.iloc[0, 1]
    new_df.iloc[0, 1] = new_df.iloc[0, 2].split('=')[0].rstrip()
    duration_list = []
    minute_format = '%H:%M:%S'
    for step in np.unique(new_df.Process_stage):
        sub_df = new_df[new_df.Process_stage == step]
        n = sub_df.shape[0]
        start = datetime.datetime.strptime(sub_df.iloc[0, 4], minute_format)
        end = datetime.datetime.strptime(sub_df.iloc[-1, 4], minute_format)
        delta = end - start
        duration = delta.total_seconds()
        duration_list = sum([duration_list, list(np.repeat(duration, n))], [])
    new_df['Duration'] = duration_list
    return new_df


def load_result_file_in_working_directory(result_file_name: str, hive_working_dir: str):
    """

    :param result_file_name: the name of the csv file resuming the control
    :param hive_working_dir: My working directory on HIVE
    :return: Copy the file path in the hive architecture
    """
    result_file_dir = delete_existing_dir(hive_working_dir, result_file_name)
    command_dir = "hdfs dfs -mkdir " + result_file_dir
    subprocess.call(command_dir, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    command = "hdfs dfs -copyFromLocal -f " + result_file_name + " " + result_file_dir + "/" + result_file_name
    subprocess.call(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    result_file_name_path = hive_working_dir + "/" + result_file_name + result_file_name.split('.')[0] + "/" \
                            + result_file_name
    return result_file_name_path


def delete_existing_dir(hive_working_dir: str, result_file_name: str):
    """

    :param hive_working_dir: My working directory on HIVE
    :param result_file_name: the name of the csv file resuming the control
    :return: delete the directory if it already exists
    """
    result_file_dir = hive_working_dir + "/" + result_file_name.split('.')[0]
    exist_command = "hdfs dfs -d " + result_file_dir + "/" + " | echo $?"
    res = subprocess.check_output(exist_command, shell=True)
    if res == 0:
        delete_command = "hdfs dfs -rm -r " + result_file_dir
        subprocess.call(delete_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    return result_file_dir


def update_file_name(log_file_name: str, log_table_file_name: str):
    """

    :param log_file_name: the name of the log file generated by ccopyFile.sh
    :param log_table_file_name: the name of the csv file representing the pandas dataframe
    :return: the new name of the log table file with the date
    """
    split_list = log_file_name.split('_')
    new_log_table_file_name = log_table_file_name.split('.')[0] + '_' + split_list[1] + '.log'
    return new_log_table_file_name


log_file_name = sys.argv[1]
df = parse_log(log_file_name)
final_df = modify_data_frame(df)
log_table_file_name = 'copy_file_log_summary.csv'
log_table_file_name = update_file_name(log_file_name, log_table_file_name)
final_df.to_csv(log_table_file_name, index=False)
# load the result into the working directory on the platform
hive_working_dir = '/user/ctqt****/WORK/results'
load_result_file_in_working_directory(result_file_name=log_table_file_name, hive_working_dir=hive_working_dir)


"""
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sqlContext.sql("use default")
DataFrame[result: string]
sqlContext.sql("CREATE EXTERNAL TABLE copy_file_log_table(
Status STRING,
Process_stage STRING,
Contents STRING,
Production_date STRING,
Production_time STRING
)
ROW FORMAT DELIMITED
fields terminated by ','
STORED AS TEXTFILE
location '/user/ctqt****/WORK/results/copy_file_log_summary/'
tblproperties ('skip.header.line.count' = '1')")

"""