#!/usr/bin/env python

from lab_utils import *

import luigi
from jinja2 import Template


# ###################
# AUXILIARY FUNCTIONS
# ###################

# Renders the table name for each different indicator
def get_table_name(test_prefix):
    return f'{test_prefix}covid_survey_json'

INDICATORS = (
    'covid',
    'mask',
    'tested_positive_14d',
    'anosmia',
)


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
        return f"survey_date = {self.date.replace('-','')}"

    def requires(self):
        return CreateTable(test_prefix=self.test_prefix)

    def run(self):

        data_date = self.date.replace('-','')

        JOIN_STATEMENT = f'{INDICATORS[0]}'
        for i in range(1, len(INDICATORS)):
            # NOTE: SQLite doesn't support FULL OUTER JOIN
            JOIN_STATEMENT += """
                LEFT JOIN {i1}
                ON {i0}.iso_code = {i1}.iso_code
                AND {i0}.survey_date = {i1}.survey_date
            """.format(i0=INDICATORS[0], i1=INDICATORS[i])

        WITH_STATEMENT = ',\n'.join(['{i} AS (SELECT * FROM rpl_covid_survey_{i} WHERE survey_date={data_date})'.format(i=i, data_date=data_date) for i in INDICATORS])

        # String manipulation to concatenate different indicators withing a JSON-formatted string
        percent_json = ' || '.join([""" '"{i}": ' || COALESCE(CAST({percent}_{code} AS TEXT), 'null') """.format(i=i, code=get_indicator_code(i), percent = 'pct' if i == 'covid' else 'percent') for i in INDICATORS])
        json_se = ' || '.join([""" '"{i}": ' || COALESCE(CAST({code}_se AS TEXT), 'null') """.format(i=i, code=get_indicator_code(i)) for i in INDICATORS])
        percent_json_unw = ' || '.join([""" '"{i}": ' || COALESCE(CAST({percent}_{code}_unw AS TEXT), 'null') """.format(i=i, code=get_indicator_code(i), percent = 'pct' if i == 'covid' else 'percent') for i in INDICATORS])
        json_se_unw = ' || '.join([""" '"{i}": ' || COALESCE(CAST({code}_se_unw AS TEXT), 'null') """.format(i=i, code=get_indicator_code(i)) for i in INDICATORS])
        json_sample_size = ' || '.join([""" '"{i}": ' || COALESCE(CAST({i}.sample_size AS TEXT), 'null') """.format(i=i) for i in INDICATORS])

        country = 'COALESCE(%s)' % ', '.join([f'{i}.country' for i in INDICATORS])
        iso_code = 'COALESCE(%s)' % ', '.join([f'{i}.iso_code' for i in INDICATORS])
        gid_0 = 'COALESCE(%s)' % ', '.join([f'{i}.gid_0' for i in INDICATORS])

        # Delete data from the data we are inserting into (overwrite)
        run_query(f"DELETE FROM {get_table_name(self.test_prefix)} WHERE {self.get_sql_filter()}")
        # Insert data
        query = Template("""
            WITH {{WITH_STATEMENT}}
            INSERT INTO {{table_name}}
            SELECT
                '{' || {{percent_json}} || '}'
                AS percent_json,
                '{' || {{json_se}} || '}'
                AS json_se,
                '{' || {{percent_json_unw}} || '}'
                AS percent_json_unw,
                '{' || {{json_se_unw}} || '}'
                AS json_se_unw,
                '{' || {{json_sample_size}} || '}'
                AS json_sample_size,
                {{country}} AS country, 
                {{iso_code}} AS iso_code,
                {{gid_0}} AS gid_0,
                {{data_date}} AS survey_date
            FROM {{JOIN_STATEMENT}}
        """).render(
            table_name=get_table_name(self.test_prefix),
            data_date=data_date,
            WITH_STATEMENT=WITH_STATEMENT,
            JOIN_STATEMENT=JOIN_STATEMENT,
            percent_json=percent_json,
            json_se=json_se,
            percent_json_unw=percent_json_unw,
            json_se_unw=json_se_unw,
            json_sample_size=json_sample_size,
            country=country,
            iso_code=iso_code,
            gid_0=gid_0,
        )
        print(query)
        run_query(query)

    def output(self):
        return DataExists(table_name=get_table_name(self.test_prefix), where_clause=self.get_sql_filter())


if __name__ == '__main__':
     luigi.build([LoadTable()], workers=5, local_scheduler=True)
