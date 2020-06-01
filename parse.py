# From https://gist.github.com/zmjones/8862947

import csv
import pandas as pd
import requests
from ipaddress import ip_network, ip_address
from urllib.parse import urlparse
from datetime import datetime
from sys import argv
from elasticsearch6 import Elasticsearch
from elasticsearch6.helpers import scan
from argparse import ArgumentParser


def get_args():
    parser = ArgumentParser()
    parser.add_argument('--elasticsearch-url', required=True)
    parser.add_argument('--start-date', help='YYYYMMDD')
    parser.add_argument('--end-date', help='YYYYMMDD')
    args = parser.parse_args()
    return args


def get_aws_cidr_blocks():
    response = requests.get('https://ip-ranges.amazonaws.com/ip-ranges.json')
    response.raise_for_status()
    data = response.json()
    aws_cidr_blocks = {}
    for item in data['prefixes']:
        if item['service'] == 'AMAZON':
            block = ip_network(item['ip_prefix'])
            aws_cidr_blocks[block] = item['region']
    for item in data['ipv6_prefixes']:
        if item['service'] == 'AMAZON':
            block = ip_network(item['ipv6_prefix'])
            aws_cidr_blocks[block] = item['region']
    return aws_cidr_blocks


def get_aws_region(ip, blocks):
    addr = ip_address(ip)
    for block, region in blocks.items():
        if addr in block:
            return region
    return ''


def get_records(report_date, elasticsearch_url):
    query = {
        'query': {
            'bool': {
                'filter': [
                    {
                        'range': {
                            'date': {
                                'gte': report_date.strftime('%Y-%m-%d'),
                                'lte': report_date.strftime('%Y-%m-%d'),
                            },
                        },
                    },
                    {
                        'match': {
                            'action': 'REST.GET.OBJECT',
                        },
                    },
                    {
                        'range': {
                            'size': {
                                'gte': 2097152,
                            },
                        },
                    },
                    {
                        'bool': {
                            'minimum_should_match': 1,
                            'should': [
                                {
                                    'match': {
                                        'response': 200,
                                    },
                                },
                                {
                                    'match': {
                                        'response': 206,
                                    },
                                },
                            ],
                        },
                    },
                    {
                        'bool': {
                            'minimum_should_match': 1,
                            'should': [
                                {
                                    'match_phrase': {
                                        'bucket': f'asf-ngap2-p-*',
                                    },
                                },
                                {
                                    'match_phrase': {
                                        'bucket': f'asf-ngap2w-p-*',
                                    },
                                },
                            ],
                        },
                    },
                ],
                'must_not': [
                    {
                        'match_phrase_prefix': {
                            'user_agent': 'RAIN Egress App for userid=',
                        },
                    },
                    {
                        'match_phrase_prefix': {
                            'user_agent': 'Egress App for userid=',
                        },
                    },
                ],
            },
        },
    }

    desired_fields = ['ip', 'object', 'response', 'volume', 'size', 'user_agent', 'userid', 'date', 'eventid']
    index = f'dls.*'

    es_client = Elasticsearch(elasticsearch_url)
    results = scan(es_client, query=query, index=index, doc_type='log', _source_includes=desired_fields)
    records = (r['_source'] for r in results)
    return records


def create_data_frame(log_entries):
    df = pd.DataFrame.from_dict(log_entries)
    df.drop_duplicates(subset='eventid', inplace=True)

    df['date'] = df.date.apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d'))
    df['user_agent'] = df.user_agent.apply(lambda x: str(x).split('/')[0])

    return df


def output_to_csv(df, output_file_name):
    final = df.groupby(['userid', 'ip', 'object', 'size', 'user_agent', 'date'])
    final = final.agg({'volume': 'sum', 'eventid': 'count'})
    final = final.reset_index()

    final['product_type'] = final.object.apply(lambda x: x.split('_')[2])
    final['aquisition_date'] = final.object.apply(lambda x: datetime.strptime(x[17:25], '%Y%m%d'))
    final['percent_of_file_downloaded'] = final.apply(lambda x: float(x.volume) / float(x['size']), axis=1)
    final['product_age_in_days_at_time_of_download'] = final.apply(lambda x: (x.date - x.aquisition_date).days, axis=1)

    aws_cidr_blocks = get_aws_cidr_blocks()
    region_map = {}
    for ip in final.ip.unique():
        region_map[ip] = get_aws_region(ip, aws_cidr_blocks)
    final['aws_region'] = final.ip.apply(lambda x: region_map[x])

    final.to_csv(output_file_name, index=False)


if __name__ == '__main__':
    args = get_args()
    daterange = pd.date_range(args.start_date, args.end_date)
    for day in daterange:
        print(day.strftime('%Y%m%d'))
        log_entries = get_records(day, args.elasticsearch_url)
        df = create_data_frame(log_entries)
        output_to_csv(df, f'{day.strftime("%Y%m%d")}.csv')
