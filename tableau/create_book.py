# -*- coding: utf-8 -*-
import codecs
import base64
import uuid
import zipfile

from tableaudocumentapi import Datasource
import tableauserverclient as TSC

PROJECT_NAME = 'economics'
SERVER = TSC.Server('http://127.0.0.1:8000')
AUTH = TSC.TableauAuth('vladimir', base64.decodestring('LGVodWV5bGJ6NDc='))


def publish_book(name, db, key_template):
    # profit_template.twbx


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


def _publish_datasource(name, items):
    project_id = _get_project_id()
    datasource_file = _fill_datasource(name, items)
    datasource = TSC.DatasourceItem(project_id, name=name)
    with SERVER.auth.sign_in(AUTH):
        datasource = SERVER.datasources.publish(
            datasource, datasource_file, 'Overwrite')
    return datasource


def _fill_datasource(name, items):
    ds = Datasource.from_file('series_profits_template.tdsx')
    Datasource.save_as(ds, '{}.tdsx'.format(name))
    datasource_file = '{}.tdsx'.format(name)
    zip_file = zipfile.ZipFile(datasource_file, 'a')
    header = (
        'Number of Records;Redis Key;Author;Date;Id;Is Profit;Model Name User;'
        'Profit Company;Profit Country;Profit Currency;Profit Industry;'
        'Profit Sector;Series Name User;Series Type;Value')
    data = header
    if items:
        data += '\n'.join(items)
    zip_file.writestr(
        'Data/tableau/series_profits.csv',
        '{}{}'.format(codecs.BOM_UTF16_LE, data.encode('utf-16-le')))
    zip_file.close()
    return datasource_file


if __name__ == '__main__':
    print(publish('testDS', [
        '1;series_profits:7;macbook;02.01.2018;0;True;;other;USA;USD;other;other;USA_profit_other;other_profit;1704',
        '1;series_profits:31;macbook;02.01.2018;13;True;Exxon_4;Exxon;USA;USD;Oil;Public;incomePerDay_exxon;output;200',
        '1;series_profits:21;macbook;03.01.2018;10;True;China_profit;Yuan;China;ALL;ALL;ALL;China_income_per_day;output;50']))
