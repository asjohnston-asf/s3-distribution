# From https://gist.github.com/zmjones/8862947

import csv
import re
import dateutil
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
    aws_cidr_blocks = []
    for item in data['prefixes']:
        if 'ip_prefix' in item:
            block = {
                'block': ip_network(item['ip_prefix']),
                'region': item['region'],
            }
            aws_cidr_blocks.append(block)
    return aws_cidr_blocks


def get_aws_region(ip, blocks):
    addr = ip_address(ip)
    for block in blocks:
        if addr in block['block']:
            return block['region']
    return ''


def get_log_entries(log_file):
    log_entries = []
    r = csv.reader(open(log_file), delimiter=' ', quotechar='"')
    for i in r:
        i[2] = i[2] + ' ' + i[3]  # repair date field
        del i[3]
        if len(i) == 18:
            log_entries.append(i)
    return log_entries


def create_data_frame(log_entries):
    columns = ['Bucket_Owner', 'Bucket', 'Time', 'Remote_IP', 'Requester',
               'Request_ID', 'Operation', 'Key', 'Request_URI', 'HTTP_status',
               'Error_Code', 'Bytes_Sent', 'Object_Size', 'Total_Time',
               'Turn_Around_Time', 'Referrer', 'User_Agent', 'Version_Id']
    df = pd.DataFrame(log_entries, columns=columns)
    df.drop(['Bucket_Owner', 'Requester', 'Request_ID', 'Operation', 'HTTP_status', 'Error_Code', 'Total_Time', 'Turn_Around_Time', 'Version_Id'], axis=1)
    df = df.mask(df == '-')
    df['Bytes_Sent'].fillna(0, inplace=True)
    df['Bytes_Sent'] = df.Bytes_Sent.astype(int)
    df['Object_Size'] = df.Object_Size.astype(int)
    return df


def add_computed_fields(df):
    userid_pattern = re.compile('&userid=(\S+) ')
    df['User_Id'] = df.Request_URI.apply(lambda x: userid_pattern.search(x).group(1))
    df['Time'] = df.Time.map(lambda x: x[x.find('[') + 1:x.find(' ')])
    df['Time'] = df.Time.map(lambda x: re.sub(':', ' ', x, 1))
    df['Time'] = df.Time.apply(dateutil.parser.parse)
    df['Referrer'] = df.Referrer.apply(lambda x: urlparse(x).hostname if x == x else '')
    df['User_Agent'] = df.User_Agent.apply(lambda x: str(x).split('/')[0] if x == x else '')
    df['Granule_Time'] = df.Key.apply(lambda x: datetime.strptime(x[17:32], '%Y%m%dT%H%M%S'))
    df['Product_Age'] = df.apply(lambda x: (x.Time - x.Granule_Time).days, axis=1)


def output_to_csv(df, output_file_name):
    final = df.groupby(['User_Id', 'Remote_IP', 'Bucket', 'Key', 'Object_Size', 'Referrer', 'User_Agent', 'Product_Age'])
    final = final.agg({'Time': 'min', 'Bytes_Sent': 'sum'})
    final = final.reset_index()
    aws_cidr_blocks = get_aws_cidr_blocks()
    final['Platform'] = final.Key.apply(lambda x: x.split('_')[0])
    final['Beam_Mode'] = final.Key.apply(lambda x: x.split('_')[1])
    final['Product_Type'] = final.Key.apply(lambda x: x.split('_')[2])
    final['Percent_Downloaded'] = final.apply(lambda x: float(x.Bytes_Sent) / float(x.Object_Size), axis=1)
    final['AWS_Region'] = final.Remote_IP.apply(lambda x: get_aws_region(x, aws_cidr_blocks))
    final.to_csv(output_file_name, index=False)


if __name__ == '__main__':
    input_file_name = argv[1]
    output_file_name = argv[2]
    log_entries = get_log_entries(input_file_name)
    df = create_data_frame(log_entries)
    add_computed_fields(df)
    output_to_csv(df, output_file_name)
