#!/usr/bin/env python

from dsa.lab_utils import *

import luigi
import requests
import json


# ###################
# AUXILIARY FUNCTIONS
# ###################

# Renders the table name for each different indicator
def get_table_name(test_prefix):
    return f'{test_prefix}covid_survey_json'


# ##############
# PIPELINE TASKS
# ##############

class CreateTable(luigi.Task):
    """
    Creates table in database.
    """
    test_prefix = luigi.Parameter(default='')

    def run(self):
        table_schema = (
            ('percent_json', 'json'),
            ('json_se', 'json'),
            ('percent_json_unw', 'json'),
            ('json_se_unw', 'json'),
            ('json_sample_size', 'json'),
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
        return f"survey_date = {self.date.replace('-', '')}"

    def requires(self):
        return CreateTable(test_prefix=self.test_prefix)

    def run(self):
        # Detele data from the data we are inserting into (overwrite)
        run_query(f"DELETE FROM {get_table_name(self.test_prefix)} WHERE {self.get_sql_filter()}")
        # Insert data
        run_query("""
            INSERT INTO {table_name}
            SELECT 
                JSON(
                    CONCAT('{{"covid": ', percent_cli, ', "mask": ', percent_mc, '}}')
                ) AS percent_json,
                JSON(
                    CONCAT('{{"covid": ', cli_se, ', "mask": ', mc_se, '}}')
                ) AS json_se,
                JSON(
                    CONCAT('{{"covid": ', percent_cli_unw, ', "mask": ', percent_mc_unw, '}}')
                ) AS percent_json_unw,
                JSON(
                    CONCAT('{{"covid": ', cli_se_unw, ', "mask": ', mc_se_unw, '}}')
                ) AS json_se_unw,
                JSON(
                    CONCAT('{{"covid": ', a.sample_size, ', "mask": ', b.sample_size, '}}')
                ) AS json_sample_size,
                a.country, 
                a.iso_code,
                a.gid_0,
                a.survey_date
            FROM rpl_covid_survey_covid a
            INNER JOIN rpl_covid_survey_mask b
            ON a.survey_date = b.survey_date
            AND a.iso_code = b.iso_code
            WHERE
                a.survey_date = {data_date}
        """.format(
            table_name=get_table_name(self.test_prefix),
            data_date=self.date.replace('-', '')
        ))

    def output(self):
        return DataExists(table_name=get_table_name(self.test_prefix), where_clause=self.get_sql_filter())


if __name__ == '__main__':
    luigi.build([LoadTable()], workers=5, local_scheduler=True)
