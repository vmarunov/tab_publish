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


def publish_book(name, db, key_template, header, formats):
    project_id = _get_project_id()
    data_frame = _read_items(db, key_template, header, formats)
    extract_file = _fill_extract(data_frame)
    workbook_file = _create_workbook(extract_file)
    workbook = TSC.WorkbookItem(project_id, name=name)
    with SERVER.auth.sign_in(AUTH):
        workbook = SERVER.workbooks.publish(
            workbook, workbook_file, TSC.Server.PublishMode.Overwrite)
    os.remove(workbook_file)
    return URL.format()


def _read_items(db, key_template, header, formats):
    items = dict(zip(header, [] * len(header)))
    items['RedisKey'] = []
    for key in db.scan_iter(match=key_template, count=PART_COUNT):
        redis_hash = db.hgetall(key)
        for field in header:
            items[field].append(redis_hash.get(field))
        items['RedisKey'].append(key)

    data_frame = pd.DataFrame(items)
    # Format data in pandas
    for field, field_type, kwargs in formats.items():
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
    header = [
        'RedisKey', 'author', 'date_', 'id', 'is_profit', 'model_name_user',
        'profit_company', 'profit_country', 'profit_currency',
        'profit_industry', 'profit_sector', 'series_name_user', 'series_type',
        'value']
    formats = {
        'date_': ('date', {'format': '%d.%m.%Y'}),
        'value': ('numeric', None)}
    db = redis.Redis(password="qwe")

    data = [
        'series_profits:7;macbook;02.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;1704',
        'series_profits:31;macbook;02.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;200',
        'series_profits:21;macbook;03.01.2018;10;True;China_profit;Yuan;China;ALL;ALL;ALL;China_income_per_day;output;50',
        'series_profits:9;macbook;03.01.2018;0;True;;ORACLE;USA;USD;Software;Public;ORACLE_profit_per_day;timeseries;5',
        'series_profits:16;macbook;01.01.2018;0;True;;Microsoft;USA;USD;Software;Public;MSFT_profit_per_day;timeseries;0',
        'series_profits:5;macbook;03.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;1452,9',
        'series_profits:25;macbook;03.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;50',
        'series_profits:4;macbook;01.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;0',
        'series_profits:27;macbook;02.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;50',
        'series_profits:28;macbook;01.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;0',
        'series_profits:26;macbook;04.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;50',
        'series_profits:2;macbook;04.01.2018;18;True;Goodyear;Goodyear;USA;USD;Tyres;Public;incomePerDay_goodyear;output;300',
        'series_profits:14;macbook;04.01.2018;0;True;;ALL;USA;USD;ALL;ALL;USA_profit;timeseries;2000',
        'series_profits:20;macbook;01.01.2018;10;True;China_profit;Yuan;China;ALL;ALL;ALL;China_income_per_day;output;0',
        'series_profits:15;macbook;02.01.2018;0;True;;ALL;USA;USD;ALL;ALL;USA_profit;timeseries;2000',
        'series_profits:17;macbook;03.01.2018;0;True;;Microsoft;USA;USD;Software;Public;MSFT_profit_per_day;timeseries;12,1',
        'series_profits:6;macbook;04.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;1482',
        'series_profits:11;macbook;02.01.2018;0;True;;ORACLE;USA;USD;Software;Public;ORACLE_profit_per_day;timeseries;5',
        'series_profits:29;macbook;03.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;200',
        'series_profits:23;macbook;02.01.2018;10;True;China_profit;Yuan;China;ALL;ALL;ALL;China_income_per_day;output;50',
        'series_profits:30;macbook;04.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;200',
        'series_profits:13;macbook;03.01.2018;0;True;;ALL;USA;USD;ALL;ALL;USA_profit;timeseries;2000',
        'series_profits:24;macbook;01.01.2018;0;True;;other;China;ALL;other;other;China_income_per_day_other;other_profit;0',
        'series_profits:0;macbook;01.01.2018;18;True;Goodyear;Goodyear;USA;USD;Tyres;Public;incomePerDay_goodyear;output;0',
        'series_profits:3;macbook;02.01.2018;18;True;Goodyear;Goodyear;USA;USD;Tyres;Public;incomePerDay_goodyear;output;80'
    ]
    # Удаляем из Redis существующие данные из series_profits:*

    url = publish_book('TestBook', db, 'series_profits:*', header, formats)
    print(url)
