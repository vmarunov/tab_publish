# -*- coding: utf-8 -*-
import base64
import shutil
import uuid
import zipfile

import pandas as pd
import tableauserverclient as TSC

from pandleau.pandleau import *


PROJECT_NAME = 'economics'
TABLEAU_HOST = 'http://127.0.0.1:8000'
SERVER = TSC.Server(TABLEAU_HOST)
URL = 'http://{host}/#/views/{name}/Sheet1'
AUTH = TSC.TableauAuth('vladimir', base64.decodestring('LGVodWV5bGJ6NDc='))
PART_COUNT = 1000
EXTRACT_NAME = 'series_profits (Redis.series_profits) (CData).hyper'
TEMPLATE_TWBX = 'series_profits_template.twbx'
HEADER = [
    'author', 'date_', 'id', 'is_profit', 'model_name_user', 'profit_company',
    'profit_country', 'profit_currency', 'profit_industry', 'profit_sector',
    'series_name_user', 'series_type', 'value']
FORMATS = {
    'date_': ('date', {'format': '%d.%m.%Y'}),
    'value': ('numeric', None)}


def publish_book(name, db, key_template, header=HEADER, formats=FORMATS):
    project_id = _get_project_id()
    data_frame = _read_items(db, key_template, header, formats)
    extract_file = _fill_extract(data_frame)
    workbook_file = _create_workbook(extract_file)
    workbook = TSC.WorkbookItem(project_id, name=name)
    with SERVER.auth.sign_in(AUTH):
        workbook = SERVER.workbooks.publish(
            workbook, workbook_file, TSC.Server.PublishMode.Overwrite)
    os.remove(workbook_file)
    return URL.format(host=TABLEAU_HOST, name=name)


def _read_items(db, key_template, header, formats):
    items = dict(zip(header, [[] for _ in range(len(header))]))
    items['RedisKey'] = []
    for key in db.scan_iter(match=key_template, count=PART_COUNT):
        redis_hash = db.hgetall(key)
        for field in header:
            items[field].append(redis_hash.get(field))
        items['RedisKey'].append(key)

    data_frame = pd.DataFrame(items)
    # Format data in pandas
    for field, (field_type, kwargs) in formats.items():
        if field_type == 'date':
            data_frame[field] = pd.to_datetime(data_frame[field], **kwargs)
        elif field_type == 'numeric':
            data_frame[field] = pd.to_numeric(data_frame[field])

    return data_frame


def _list_projects():
    with SERVER.auth.sign_in(AUTH):
        projects, _ = SERVER.projects.get()
    return projects


def _create_project():
    with SERVER.auth.sign_in(AUTH):
        project = SERVER.projects.create(
            TSC.ProjectItem(
                name=PROJECT_NAME,
                description='This is description',
                content_permissions='ManagedByOwner'))
    return project


def _get_project_id():
    projects = [
        project for project in _list_projects()
        if project.name == PROJECT_NAME]
    project = projects[0] if projects else _create_project()
    return project.id


def _fill_extract(data_frame):
    df_tableau = pandleau(data_frame)

    extract_file_name = '{}.hyper'.format(str(uuid.uuid4()))
    df_tableau.to_tableau(extract_file_name)
    return extract_file_name


def _create_workbook(extract_file):
    workbook_name = '{}.twbx'.format(str(uuid.uuid4()))
    shutil.copyfile(TEMPLATE_TWBX, workbook_name)
    with zipfile.ZipFile(workbook_name, 'a') as zfile:
        zfile.write(extract_file, 'Data/Datasources/{}'.format(EXTRACT_NAME))
    os.remove(extract_file)
    return workbook_name


if __name__ == '__main__':
    import redis
    db = redis.Redis(password="qwe")

    data = [
        'macbook;02.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;1704',
        'macbook;02.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;200',
        'macbook;03.01.2018;10;True;China_profit;Yuan;China;ALL;ALL;ALL;China_income_per_day;output;50',
        'macbook;03.01.2018;0;True;;ORACLE;USA;USD;Software;Public;ORACLE_profit_per_day;timeseries;5',
        'macbook;01.01.2018;0;True;;Microsoft;USA;USD;Software;Public;MSFT_profit_per_day;timeseries;0',
        'macbook;03.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;1452.9',
        'macbook;03.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;50',
        'macbook;01.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;0',
        'macbook;02.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;50',
        'macbook;01.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;0',
        'macbook;04.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;50',
        'macbook;04.01.2018;18;True;Goodyear;Goodyear;USA;USD;Tyres;Public;incomePerDay_goodyear;output;300',
        'macbook;04.01.2018;0;True;;ALL;USA;USD;ALL;ALL;USA_profit;timeseries;2000',
        'macbook;01.01.2018;10;True;China_profit;Yuan;China;ALL;ALL;ALL;China_income_per_day;output;0',
        'macbook;02.01.2018;0;True;;ALL;USA;USD;ALL;ALL;USA_profit;timeseries;2000',
        'macbook;03.01.2018;0;True;;Microsoft;USA;USD;Software;Public;MSFT_profit_per_day;timeseries;12.1',
        'macbook;04.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;1482',
        'macbook;02.01.2018;0;True;;ORACLE;USA;USD;Software;Public;ORACLE_profit_per_day;timeseries;5',
        'macbook;03.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;200',
        'macbook;02.01.2018;10;True;China_profit;Yuan;China;ALL;ALL;ALL;China_income_per_day;output;50',
        'macbook;04.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;200',
        'macbook;03.01.2018;0;True;;ALL;USA;USD;ALL;ALL;USA_profit;timeseries;2000',
        'macbook;01.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;0',
        'macbook;01.01.2018;18;True;Goodyear;Goodyear;USA;USD;Tyres;Public;incomePerDay_goodyear;output;0',
        'macbook;02.01.2018;18;True;Goodyear;Goodyear;USA;USD;Tyres;Public;incomePerDay_goodyear;output;80'
    ]
    key_template = 'series_profits:*'

    # Удаляем из Redis существующие данные из series_profits:*
    for key in db.scan_iter(match=key_template, count=PART_COUNT):
        db.delete(key)

    # Заполняем тестовыми данными
    cnt = 1
    for row in data:
        redis_hash = dict(zip(HEADER, row.split(';')))
        key = 'series_profits:{}'.format(cnt)
        db.hmset(key, redis_hash)
        cnt += 1

    url = publish_book('TestBook', db, key_template)
    print(url)
