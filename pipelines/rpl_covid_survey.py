#!/usr/bin/env python

from lab_utils import *

import luigi
import requests
import json
import os
import pandas as pd


# ###################
# AUXILIARY FUNCTIONS
# ###################

# Renders the file path for the report to download
def get_file_path(indicator, country, date):
    date_no_dash = date.replace('-', '')
    return os.path.join(RAW_DATA_FOLDER_PATH,
                        f'covid_survey__{indicator}__{country}__{date_no_dash}__{date_no_dash}.txt')


# Renders the table name for each different indicator
def get_rpl_covid_survey_table_name(indicator, test_prefix):
    return f'{test_prefix}rpl_covid_survey_{indicator}'


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
        percent = get_pct_prefix(self.indicator)
        table_schema = (
            (f'{percent}_{indicator_code}', 'float'),
            (f'{indicator_code}_se', 'float'),
            (f'{percent}_{indicator_code}_unw', 'float'),
            (f'{indicator_code}_se_unw', 'float'),
            ('sample_size', 'NUMERIC'),
            ('country', 'text'),
            ('iso_code', 'text'),
            ('gid_0', 'text'),
            ('survey_date', 'NUMERIC'),
        )
        create_table(get_rpl_covid_survey_table_name(self.indicator, self.test_prefix), table_schema)

    def output(self):
        return TableExists(get_rpl_covid_survey_table_name(self.indicator, self.test_prefix))


class DownloadAPIReport(luigi.Task):
    """
    Downloads report from API.
    """
    indicator = luigi.Parameter()
    country = luigi.Parameter()
    date = luigi.Parameter()

    def run(self):
        # Create folder is not existing
        if RAW_DATA_FOLDER_PATH not in os.listdir():
            os.mkdir(RAW_DATA_FOLDER_PATH)
        file_path = get_file_path(self.indicator, self.country, self.date)
        # Get params for API call
        date_no_dash = self.date.replace('-', '')
        url = f"https://covidmap.umd.edu/api/resources?indicator={self.indicator}&type=daily&country={self.country}&daterange={date_no_dash}-{date_no_dash}"
        # Call API and save respond in CSV
        print("CALLING API: ", url)
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
        return f"country='{self.country}' AND survey_date = {self.date.replace('-', '')}"

    def requires(self):
        yield DownloadAPIReport(indicator=self.indicator, country=self.country, date=self.date)
        yield CreateTable(indicator=self.indicator, test_prefix=self.test_prefix)

    def run(self):
        file_path = get_file_path(self.indicator, self.country, self.date)
        indicator_code = get_indicator_code(self.indicator)
        percent = get_pct_prefix(self.indicator)
        table_schema = (
            (f'{percent}_{indicator_code}', 'float'),
            (f'{indicator_code}_se', 'float'),
            (f'{percent}_{indicator_code}_unw', 'float'),
            (f'{indicator_code}_se_unw', 'float'),
            ('sample_size', 'NUMERIC'),
            ('country', 'text'),
            ('iso_code', 'text'),
            ('gid_0', 'text'),
            ('survey_date', 'NUMERIC'),
        )
        load_file_in_table(
            file_path,
            get_rpl_covid_survey_table_name(self.indicator, self.test_prefix),
            table_schema=table_schema,
            overwrite_filter=self.get_sql_filter(),
            skip_header=True,
        )

    def output(self):
        return DataExists(
            table_name=get_rpl_covid_survey_table_name(self.indicator, self.test_prefix),
            where_clause=self.get_sql_filter()
        )


class MasterTask(luigi.WrapperTask):
    """
    Generates tasks for several survey indicators and countries.
    """
    test_prefix = luigi.Parameter(default='')
    date = luigi.Parameter()

    def requires(self):
        for indicator in INDICATORS:
            for country in COUNTRIES:
                yield LoadReportIntoDB(indicator=indicator, country=country, date=self.date,
                                       test_prefix=self.test_prefix)


if __name__ == '__main__':
     luigi.build([MasterTask()], workers=5, local_scheduler=True)

# Scrip example to run the pipeline:
# python -m luigi --module pipelines.rpl_covid_survey MasterTask --date 2021-07-01 --test-prefix test_0_ --local-scheduler
