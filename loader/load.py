import pandas as pd
from redis import StrictRedis as R


# REDIS = R(password='qqq')
REDIS = R()
KEYS = ['brent', 'al', 'copper', 'benzin', 'gold']


def get_key(cnt):
    return 'commodities:{}'.format(cnt)


def _format_date(value):
    return '{}-{}-{}'.format(value[:4], value[4:6], value[-2:])


def load(f, key):
    def _s(l):
        d = l.split(',')
        return d[0], d[5]

    with open(f) as fl:
        data = fl.readlines()

    data = [_s(i) for i in data[1:]]
    return {item[0]: {key: item[1]} for item in data}


def save(df):
    with REDIS.pipeline() as pipe:
        counter = 0
        pipe.multi()
        for i, date in enumerate(df['dates']):
            for key in KEYS:
                pipe.hmset(get_key(counter), {
                    'date': date,
                    'series': key,
                    'value': df[key][i]})
                counter += 1
        pipe.execute()


if __name__ == '__main__':
    hash = dict()

    def update(d):
        for k, v in d.items():
            if k in hash:
                hash[k].update(v)
            else:
                hash[k] = v

    def get_df(d, k):
        return [hash[i].get(k) for i in d]

    update(load('brent.txt', 'brent'))
    update(load('al.txt', 'al'))
    update(load('copper.txt', 'copper'))
    update(load('benz.txt', 'benzin'))
    update(load('gold.txt', 'gold'))

    prev_date = None
    dates = sorted(hash.keys())
    for date in dates:
        hash[date] = hash[date]
        hash[date]['date'] = _format_date(date)
        if prev_date:
            for key in KEYS:
                if not hash[date].get(key):
                    hash[date][key] = hash[prev_date].get(key)
        prev_date = date

    df = pd.DataFrame({
        'dates': get_df(dates, 'date'),
        'al': get_df(dates, 'al'),
        'brent': get_df(dates, 'brent'),
        'benzin': get_df(dates, 'benzin'),
        'copper': get_df(dates, 'copper'),
        'gold': get_df(dates, 'gold')})
    save(df)
