# From https://gist.github.com/zmjones/8862947

# grep -hE 'gsfc-ngap-p-.* REST\.GET\.OBJECT .*"GET /S1.*\.zip.*userid=.*" (200|206) ' /storage/syslog/awss3access/2018/0[23456]/accesslog > access.log
# python parse.py access.log access.csv

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
    columns = ['Bucket_Owner', 'Bucket', 'Time', 'Time_Zone', 'Remote_IP', 'Requester',
               'Request_ID', 'Operation', 'Key', 'Request_URI', 'HTTP_status',
               'Error_Code', 'Bytes_Sent', 'Object_Size', 'Total_Time',
               'Turn_Around_Time', 'Referrer', 'User_Agent', 'Version_Id']
    df = pd.DataFrame(log_entries, columns=columns)

    df = df.mask(df == '-')
    df['Bytes_Sent'].fillna(0, inplace=True)
    df['Bytes_Sent'] = df.Bytes_Sent.astype(int)

    userid_pattern = re.compile('&userid=(\S+) ')
    df['User_Id'] = df.Request_URI.apply(lambda x: userid_pattern.search(x).group(1))
    df['Time'] = df.Time.apply(lambda x: datetime.strptime(x[1:12], '%d/%b/%Y'))
    df['Referrer'] = df.Referrer.apply(lambda x: urlparse(x).hostname if x == x else '')
    df['User_Agent'] = df.User_Agent.apply(lambda x: str(x).split('/')[0] if x == x else '')

    return df


def output_to_csv(df, output_file_name):
    final = df.groupby(['User_Id', 'Remote_IP', 'Bucket', 'Key', 'Object_Size', 'Referrer', 'User_Agent', 'Time'])
    final = final.agg({'Bytes_Sent': 'sum', 'Request_URI': 'count'})
    final = final.reset_index()

    final['Platform'] = final.Key.apply(lambda x: x.split('_')[0])
    final['Beam_Mode'] = final.Key.apply(lambda x: x.split('_')[1])
    final['Product_Type'] = final.Key.apply(lambda x: x.split('_')[2])
    final['Granule_Time'] = final.Key.apply(lambda x: datetime.strptime(x[17:25], '%Y%m%d'))
    final['Percent_Downloaded'] = final.apply(lambda x: float(x.Bytes_Sent) / float(x.Object_Size), axis=1)
    final['Product_Age'] = final.apply(lambda x: (x.Time - x.Granule_Time).days, axis=1)

    aws_cidr_blocks = get_aws_cidr_blocks()
    m = {}
    for ip in final.Remote_IP.unique():
        m[ip] = get_aws_region(ip, aws_cidr_blocks)
    final['AWS_Region'] = final.Remote_IP.apply(lambda x: m[x])

    final.to_csv(output_file_name, index=False)


if __name__ == '__main__':
    input_file_name = argv[1]
    output_file_name = argv[2]
    log_entries = get_log_entries(input_file_name)
    df = create_data_frame(log_entries)
    output_to_csv(df, output_file_name)
