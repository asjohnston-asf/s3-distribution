# From https://gist.github.com/zmjones/8862947

import csv
import re
import dateutil
import pandas as pd
from urlparse import urlparse
from datetime import datetime
from sys import argv


def get_log_entries(log_file):
    log_entries = []
    r = csv.reader(open(log_file), delimiter=' ', quotechar='"')
    for i in r:
        i[2] = i[2] + ' ' + i[3]  # repair date field
        del i[3]
        if len(i) <= 18:
            log_entries.append(i)
    return log_entries


    columns = ['Bucket_Owner', 'Bucket', 'Time', 'Remote_IP', 'Requester',
def create_data_frame(log_entries):
               'Request_ID', 'Operation', 'Key', 'Request_URI', 'HTTP_status',
               'Error_Code', 'Bytes_Sent', 'Object_Size', 'Total_Time',
               'Turn_Around_Time', 'Referrer', 'User_Agent', 'Version_Id']
    df = pd.DataFrame(log_entries, columns=columns)
    df = df.mask(df == '-')
    df['Bytes_Sent'].fillna(0, inplace=True)
    df['Bytes_Sent'] = df.Bytes_Sent.astype(int)
    df['Object_Size'] = df.Object_Size.astype(int)
    return df


def add_computed_fields(df):
    df['User_Id'] = df.Request_URI.apply(lambda x: re.search('&userid=(\S+) ', x).group(1))
    df['Time'] = df.Time.map(lambda x: x[x.find('[') + 1:x.find(' ')])
    df['Time'] = df.Time.map(lambda x: re.sub(':', ' ', x, 1))
    df['Time'] = df.Time.apply(dateutil.parser.parse)
    df['Referrer'] = df.Referrer.apply(lambda x: urlparse(x).hostname if x == x else 'none')
    df['User_Agent'] = df.User_Agent.apply(lambda x: str(x).split('/')[0])
    df['Granule_Time'] = df.Key.apply(lambda x: datetime.strptime(x[17:32], '%Y%m%dT%H%M%S'))
    df['Platform'] = df.Key.apply(lambda x: x.split('_')[0])
    df['Beam_Mode'] = df.Key.apply(lambda x: x.split('_')[1])
    df['Product_Type'] = df.Key.apply(lambda x: x.split('_')[2])
    df['Percent_Downloaded'] = df.apply(lambda x: float(x.Bytes_Sent) / float(x.Object_Size), axis=1)
    df['Product_Age'] = df.apply(lambda x: (x.Time - x.Granule_Time).days, axis=1)


def output_to_csv(df, output_file_name):
    grouped = df.groupby(['User_Id', 'Remote_IP', 'Key', 'Object_Size', 'Referrer', 'User_Agent', 'Product_Type', 'Beam_Mode', 'Platform', 'Product_Age'])
    final = grouped.agg({'Time': 'min', 'Percent_Downloaded': 'sum', 'Bytes_Sent': 'sum'})
    final.to_csv(output_file_name, index=True)


if __name__ == '__main__':
    input_file_name = argv[1]
    output_file_name = argv[2]
    log_entries = get_log_entries(input_file_name)
    df = create_data_frame(log_entries)
    add_computed_fields(df)
    output_to_csv(df, output_file_name)
