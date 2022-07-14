#!/usr/bin/env python

from dsa.lab_utils import *

import luigi
import requests
import json


# ###################
# AUXILIARY FUNCTIONS
# ###################

# Renders the file path for the report to download
def get_file_path(indicator, country, date):
    date_no_dash = date.replace('-', '')
    return f'data/raw/covid_survey__{indicator}__{country}__{date_no_dash}__{date_no_dash}.txt'

# Renders the table name for each different indicator
def get_table_name(indicator, test_prefix):
    return f'{test_prefix}{COVID_SURVEY_TABLE_NAME}_{indicator}'


# ##############
# PIPELINE TASKS
# ##############

class CreateTable(luigi.Task):
    """
    Creates table in database.
    """
    test_prefix = luigi.Parameter(default='')
    indicator = luigi.Parameter()

    def run(self):
        indicator_code = get_indicator_code(self.indicator)
        table_schema = (
            (f'percent_{indicator_code}', 'float'),
            (f'{indicator_code}_se', 'float'),
            (f'percent_{indicator_code}_unw', 'float'),
            (f'{indicator_code}_se_unw', 'float'),
            ('sample_size', 'NUMERIC'),
            ('country', 'text'),
            ('iso_code', 'text'),
            ('gid_0', 'text'),
            ('survey_date', 'NUMERIC'),
        )
        create_table(get_table_name(self.indicator, self.test_prefix), table_schema)

    def output(self):
        return TableExists(get_table_name(self.indicator, self.test_prefix))


class DownloadAPIReport(luigi.Task):
    """
    Downloads report from API.
    """
    indicator = luigi.Parameter()
    country = luigi.Parameter()
    date = luigi.Parameter()

    def run(self):
        date_no_dash = self.date.replace('-', '')
        file_path = get_file_path(self.indicator, self.country, self.date)
        url = f"https://covidmap.umd.edu/api/resources?indicator={self.indicator}&type=daily&country={self.country}&daterange={date_no_dash}-{date_no_dash}"
        print(url)
        response = requests.get(url).text
        response_dict = json.loads(response)
        df = pd.DataFrame.from_dict(response_dict['data'])
        df.to_csv(file_path, sep='\t', index=False, encoding='utf-8')

    def output(self):
        file_path = get_file_path(self.indicator, self.country, self.date)
        return luigi.LocalTarget(file_path)
        

class LoadReportIntoDB(luigi.Task):
    """
    Loads report file into database.
    """
    test_prefix = luigi.Parameter(default='')
    indicator = luigi.Parameter()
    country = luigi.Parameter()
    date = luigi.Parameter()

    def get_sql_filter(self):
        return f"country='{self.country}' AND survey_date = {self.date.replace('-','')}"

    def requires(self):
        yield DownloadAPIReport(indicator=self.indicator, country=self.country, date=self.date)
        yield CreateTable(indicator=self.indicator, test_prefix=self.test_prefix)

    def run(self):
        file_path = get_file_path(self.indicator, self.country, self.date)
        load_file_in_table(file_path, get_table_name(self.indicator, self.test_prefix), overwrite_filter=self.get_sql_filter())

    def output(self):
        return DataExists(table_name=get_table_name(self.indicator, self.test_prefix), where_clause=self.get_sql_filter())


class MasterTask(luigi.WrapperTask):
    """
    Generates tasks for several survey indicators and countries.
    """
    test_prefix = luigi.Parameter(default='')
    date = luigi.Parameter()

    def requires(self):
        for indicator in ['mask', 'finance']:
            for country in ['Germany', 'Japan']:
                yield LoadReportIntoDB(indicator=indicator, country=country, date=self.date, test_prefix=self.test_prefix)


if __name__ == '__main__':
     luigi.build([MasterTask()], workers=5, local_scheduler=True)
