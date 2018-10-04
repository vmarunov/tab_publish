# -*- coding: utf-8 -*-
import pandas as pd
from redis import StrictRedis

TS_KEY = 'ts:{}'
P2_KEY = 'p2:{}'

REDIS = StrictRedis()


TS = pd.DataFrame({
    'Date': ['01.01.2001', '01.02.2001', '01.01.2001', '01.02.2001'],
    'Series': ['gold', 'gold', 'oil', 'oil'],
    'Value': [50, None, 100, 30]})

P2 = pd.DataFrame({
    'Date': [
        '01.01.2001', '01.01.2001', '01.01.2001', '01.01.2001', '01.01.2001',
        '01.01.2001', '01.01.2001', '01.01.2001', '01.01.2001'],
    'Company': [
        'Microsot', 'Other', 'Other', 'Other', 'Oracle', 'Other', 'Other',
        'Other', 'Other'],
    'Profit': ['100', '300', '100', '600', '100', '400', '1500', '600', '800'],
    'Profit_Industry': [
        'Software', 'Software', 'Other', 'Oil', 'Software', 'Software',
        'Other', 'Oil', 'Other'],
    'Country': [
        'USA', 'USA', 'USA', 'USA', 'USA', 'USA', 'Other', 'USA', 'China']})


def save_data_frame(data_frame, key):
    df_keys = data_frame.keys()
    for cnt in range(len(data_frame)):
        REDIS.hmset(
            key.format(cnt),
            {df_key: data_frame[df_key][cnt] for df_key in df_keys})


if __name__ == '__main__':
    save_data_frame(TS, TS_KEY)
    save_data_frame(P2, P2_KEY)
