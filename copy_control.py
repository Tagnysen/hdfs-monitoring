"""
    Usage: spark-submit script_name raw_data_path conf_file_path
"""

import subprocess
import os
import re
import sys
import pandas as pd


def generate_raw_data_list(raw_data_dir_path: str, raw_data_file_names: str):
    """

    :param raw_data_dir_path: path of the raw data from the CFT transfer on the platform
    :param raw_data_file_names: names of the files in the directory
    :return: Generate a txt file in the working directory containing the file names
    """
    command = "hdfs dfs -du " + raw_data_dir_path + " > " + raw_data_file_names
    subprocess.call(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)


def generate_mask_file_list(conf_file_pat: str, file_list_name: str):
    """

    :param conf_file_path: the conf file in charge of the copy in the right directory
    :param file_list_name:  the list of all the file names before the copy
    :return:  method which create a txt file with the mask of tehe files supposed tobe copied
    """
    command = "hdfs dfs -cat " + conf_file_path +  " | " + \
              "awk -F \"*;\" '/^[^#]/ {printf $1 \"\\n\"}' > " + file_list_name
    subprocess.call(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)


def get_mask_list(file_list_path):
    """

    :param file_list_path: path of the txt file containing the mask of the files supposed to be copied
    :return: list of the mask prototypes
    """
    if os.path.exists(file_list_path):
        f = open(file_list_path, 'r')
        lines = f.readlines()
        f.close()
        mask_list = []
        for l in lines:
            pattern = r"(^" + l.strip() + r".*)$"
            mask_list.append(pattern)
        return mask_list
    else:
        print("No such file or directory")


def get_raw_data_name_and_volume(raw_data_file_names: str):
    """

    :param raw_data_dir_path: path of the raw data from the CFT transfer
    :return: list of the raw data (without the path)
    """
    if os.path.exists(raw_data_file_names):
        f = open(raw_data_file_names, 'r')
        lines = f.readlines()
        f.close()
        raw_data_and_volume_dico = {}
        for l in lines:
            new_line = l.strip().split(' ')
            file_name_and_volume_list = list(filter(None, new_line))
            file_name = file_name_and_volume_list[1].strip().split('/')[-1]
            volume = file_name_and_volume_list[0]
            raw_data_and_volume_dico[file_name] = volume
        return raw_data_and_volume_dico
    else:
        print("No such file or directory")


def update_file_names(raw_data_file_names: str, raw_data_dir_path: str, result_file_name: str):
    """

    :param raw_data_file_names: names of the files in the directory
    :param raw_data_dir_path: path of the raw data from the CFT transfer
    :param result_file_name: he name of the csv file resuming the control
    :return: the file names with the day of data production
    """
    split_path = raw_data_dir_path.split('/')
    day = split_path[-3] + split_path[-2] + split_path[-1]
    new_raw_data_file_names = raw_data_file_names + '_' + day + '.txt'
    new_file_result_name = result_file_name + '_' + day + '.csv'
    return new_raw_data_file_names, new_file_result_name


def control_copy_file(mask_list: list, raw_data_file_names: str):
    """

    :param mask_list: list of the patterns of the files according to the conf file
    :param raw_data_file_names: file containing the names of the raw data
    :return:
    """
    control_dico = {}
    raw_data_and_volume_dico = get_raw_data_name_and_volume(raw_data_file_names)
    raw_data_name_list = raw_data_and_volume_dico.keys()
    match_table_dico = {}
    for pattern in mask_list:
        occ = 1
        control_dico[str(pattern.strip()[2:-5])] = "NOT OK"
        match_mask_to_file_name(control_dico, match_table_dico, pattern, raw_data_name_list, mask_list, occ)
    return control_dico, match_table_dico


def match_mask_to_file_name(control_dico: dict, match_table_dico: dict, pattern: str, raw_data_name_list: list, mask_list: list, occ: int):
    """

    :param control_dico: control dictionary with file mask as keys and control result as values
    :param match_table_dico: dictionary with file mask as keys and real file name as values
    :param pattern: file mask extracted from the conf file
    :param raw_data_name_list: list of the file names present in the raw data directory
    :param mask_list: list of the patterns of the files according to the conf file
    :param occ: initial occurrence
    :return: fill in the dictionaries
    """
    match_table_dico[str(pattern.strip()[2:-5])] = []
    for file in raw_data_name_list:
        if re.match(pattern, file) is not None:
            match_table_dico[str(pattern.strip()[2:-5])].append(file)
            if mask_list.count(pattern) == occ:
                control_dico[str(pattern.strip()[2:-5])] = "OK"
            else:
                occ += 1


def control_report(mask_list: list, raw_data_file_names: str):
    """

    :param mask_list: list of the patterns of the files according to the conf file
    :param raw_data_file_names: file containing the names of the raw data
    :return: Pandas dataframe resuming the control work
    """
    raw_data_and_volume_dico = get_raw_data_name_and_volume(raw_data_file_names)
    control_dico = control_copy_file(mask_list, raw_data_file_names)[0]
    match_table_dico = control_copy_file(mask_list, raw_data_file_names)[1]
    volume_list = []
    raw_data_list = list(raw_data_and_volume_dico.keys())
    temp_list  = []
    for l in list(match_table_dico.values()):
        for v in l:
            temp_list.append(v)    
    for k, v in match_table_dico.items():
        vol = 0
        for elt in v:
            vol += int(raw_data_and_volume_dico[str(elt)])        
        volume_list.append(vol)
    new_file_names_list = []
    for names in list(match_table_dico.values()):
        new_file_names_list.append(str(names)[1:-1])
    data = {'Copy_control_result': list(control_dico.values()), 'File_name': new_file_names_list,
            'Pattern_file_name': list(control_dico.keys()),  'Volume': list(volume_list)}
    extra_raw_data = [f for f in raw_data_list if f not in temp_list]
    return pd.DataFrame(data), extra_raw_data


def load_result_file_in_working_directory(result_file_name: str, hive_working_dir: str):
    """

    :param result_file_name: the name of the csv file resuming the control
    :param hive_working_dir: My working directory on HIVE
    :return: Copy the file path in the hive architecture
    """
    result_file_dir = delete_existing_dir(hive_working_dir, result_file_name)
    command_dir = "hdfs dfs -mkdir " + result_file_dir
    subprocess.call(command_dir, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    command = "hdfs dfs -copyFromLocal -f " + result_file_name + " " + hive_working_dir + "/" + \
              result_file_name.split('.')[0] + "/" + result_file_name
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
    exist_command = "hdfs dfs -test -d " + result_file_dir + "/" + " | echo $?"
    res = subprocess.check_output(exist_command, shell=True)
    if res == 0:
        delete_command = "hdfs dfs -rm -r " + result_file_dir
        subprocess.call(delete_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    return result_file_dir


file_list_name = "mask_list.txt"
raw_data_file_names = "raw_data_list"
result_file_name = "copy_control_table"
conf_file_path = sys.argv[2]
raw_data_dir_path = sys.argv[1]
raw_data_file_names, result_file_name = update_file_names(raw_data_file_names, raw_data_dir_path, result_file_name)
file_list_path = os.path.abspath(file_list_name)


# Generate the paths of the raw data
generate_raw_data_list(raw_data_dir_path, raw_data_file_names)
# Generate the mask_list_file
generate_mask_file_list(conf_file_path, file_list_name)
# Then let's get the list of the patterns according to the conf file
mask_list = get_mask_list(file_list_path)
# Finally we can control the copy by displaying a summary data frame
data, extra_list = control_report(mask_list, raw_data_file_names)
data.to_csv(result_file_name, index=False)
print(extra_list)
# load the result file into my workspace on Hive
hive_working_dir = '/user/ctqt***/WORK/results'
load_result_file_in_working_directory(result_file_name, hive_working_dir)


"""
# def generate_hive_table():

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sqlContext.sql("use default")
DataFrame[result: string]
sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS result_file_name(
Copy_control_result STRING,
File_name STRING,
Pattern_file_name STRING,
Volume FLOAT
)
ROW FORMAT DELIMITED
fields terminated by '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/user/ctq***/WORK/results/result_file_name/'
tblproperties ('skip.header.line.count' = '1')")

"""
