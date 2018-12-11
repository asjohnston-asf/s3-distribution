# From https://gist.github.com/zmjones/8862947

import csv
import re
import pandas as pd
import requests
import json
from ipaddress import ip_network, ip_address
from urllib.parse import urlparse
from datetime import datetime
from sys import argv


def get_aws_cidr_blocks():
    response = requests.get('https://ip-ranges.amazonaws.com/ip-ranges.json')
    response.raise_for_status()
    data = json.loads(response.text)
    aws_cidr_blocks = {}
    for item in data['prefixes']:
        if 'ip_prefix' in item:
            block = ip_network(item['ip_prefix'])
            aws_cidr_blocks[block] = item['region']
    return aws_cidr_blocks


def get_aws_region(ip, blocks):
    addr = ip_address(ip)
    for block, region in blocks.items():
        if addr in block:
            return region
    return ''


def get_log_entries(log_file):
    r = csv.reader(open(log_file), delimiter=' ', quotechar='"')
    log_entries = [record for record in r if len(record) == 19]
    return log_entries


def create_data_frame(log_entries):
    columns = ['Bucket_Owner', 'Bucket', 'Request_Time', 'Time_Zone', 'IP_Address', 'Requester',
               'Request_ID', 'Operation', 'File_Name', 'Request_URI', 'HTTP_Status',
               'Error_Code', 'Bytes_Downloaded', 'File_Size', 'Total_Time',
               'Turn_Around_Time', 'Referrer', 'User_Agent', 'Version_Id']
    df = pd.DataFrame(log_entries, columns=columns)
    df.drop_duplicates(subset='Request_ID', inplace=True)

    df = df.mask(df == '-')
    df['Bytes_Downloaded'].fillna(0, inplace=True)
    df['Bytes_Downloaded'] = df.Bytes_Downloaded.astype(int)

    userid_pattern = re.compile('userid=([a-zA-Z0-9\._]+)')
    df['User_Id'] = df.Request_URI.apply(lambda x: userid_pattern.search(x).group(1))
    df['Request_Date'] = df.Request_Time.apply(lambda x: datetime.strptime(x[1:12], '%d/%b/%Y'))
    df['Referrer'] = df.Referrer.apply(lambda x: urlparse(x).hostname if x == x else '')
    df['User_Agent'] = df.User_Agent.apply(lambda x: str(x).split('/')[0] if x == x else '')

    return df


def output_to_csv(df, output_file_name):
    final = df.groupby(['User_Id', 'IP_Address', 'Bucket', 'File_Name', 'File_Size', 'Referrer', 'User_Agent', 'Request_Date'])
    final = final.agg({'Bytes_Downloaded': 'sum', 'Request_URI': 'count'})
    final = final.reset_index()

    final['Platform'] = final.File_Name.apply(lambda x: x.split('_')[0])
    final['Beam_Mode'] = final.File_Name.apply(lambda x: x.split('_')[1])
    final['Product_Type'] = final.File_Name.apply(lambda x: x.split('_')[2])
    final['Aquisition_Date'] = final.File_Name.apply(lambda x: datetime.strptime(x[17:25], '%Y%m%d'))
    final['Percent_of_File_Downloaded'] = final.apply(lambda x: float(x.Bytes_Downloaded) / float(x.File_Size), axis=1)
    final['Product_Age_in_Days_at_Time_of_Download'] = final.apply(lambda x: (x.Request_Date - x.Aquisition_Date).days, axis=1)

    aws_cidr_blocks = get_aws_cidr_blocks()
    region_map = {}
    for ip in final.IP_Address.unique():
        region_map[ip] = get_aws_region(ip, aws_cidr_blocks)
    final['AWS_Region'] = final.IP_Address.apply(lambda x: region_map[x])

    final.to_csv(output_file_name, index=False)


if __name__ == '__main__':
    input_file_name = argv[1]
    output_file_name = argv[2]
    log_entries = get_log_entries(input_file_name)
    df = create_data_frame(log_entries)
    output_to_csv(df, output_file_name)
