#!/usr/bin/env python

from lab_utils import *

import luigi
import requests
import json


# ###################
# AUXILIARY FUNCTIONS
# ###################

# Renders the table name for each different indicator
def get_table_name(test_prefix):
    return f'{test_prefix}covid_survey_covid_mask'


# ##############
# PIPELINE TASKS
# ##############

class CreateTable(luigi.Task):
    """
    Creates table in database.
    """
    test_prefix = luigi.Parameter(default='')
    def run(self):
        indicator_code = get_indicator_code('covid')
        table_schema = (
            ('percent_cli', 'float'),
            ('cli_se', 'float'),
            ('percent_cli_unw', 'float'),
            ('cli_se_unw', 'float'),
            ('cli_sample_size', 'NUMERIC'),
            ('percent_mc', 'float'),
            ('mc_se', 'float'),
            ('percent_mc_unw', 'float'),
            ('mc_se_unw', 'float'),
            ('mc_sample_size', 'NUMERIC'),
            ('country', 'text'),
            ('iso_code', 'text'),
            ('gid_0', 'text'),
            ('survey_date', 'NUMERIC'),
        )
        create_table(get_table_name(self.test_prefix), table_schema)

    def output(self):
        return TableExists(get_table_name(self.test_prefix))


class LoadTable(luigi.Task):
    """
    Loads report file into database.
    """
    test_prefix = luigi.Parameter(default='')
    date = luigi.Parameter()

    def get_sql_filter(self):
        return f"survey_date = {self.date.replace('-','')}"

    def requires(self):
        return CreateTable(test_prefix=self.test_prefix)

    def run(self):
        # Detele data from the data we are inserting into (overwrite)
        run_query(f"DELETE FROM {get_table_name(self.test_prefix)} WHERE {self.get_sql_filter()}")
        # Insert data
        run_query(f"""
            INSERT INTO {get_table_name(self.test_prefix)}
            SELECT 
                percent_cli,
                cli_se,
                percent_cli_unw,
                cli_se_unw,
                a.sample_size AS cli_sample_size,
                percent_mc,
                mc_se,
                percent_mc_unw,
                mc_se_unw,
                b.sample_size AS mc_sample_size,
                a.country, 
                a.iso_code,
                a.gid_0, 
                a.survey_date
            FROM rpl_covid_survey_covid a
            INNER JOIN rpl_covid_survey_mask b
            ON a.survey_date = b.survey_date
            AND a.iso_code = b.iso_code
            WHERE
                a.survey_date = {self.date.replace('-','')}
        """)

    def output(self):
        return DataExists(table_name=get_table_name(self.test_prefix), where_clause=self.get_sql_filter())


if __name__ == '__main__':
     luigi.build([LoadTable()], workers=5, local_scheduler=True)
