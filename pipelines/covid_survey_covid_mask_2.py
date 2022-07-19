#!/usr/bin/env python

from lab_utils import *

import luigi
from jinja2 import Template


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
            ('percent_json', 'text'),
            ('json_se', 'text'),
            ('percent_json_unw', 'text'),
            ('json_se_unw', 'text'),
            ('json_sample_size', 'text'),
            ('country', 'text'),
            ('iso_code', 'text'),
            ('gid_0', 'text'),
            ('survey_date', 'NUMERIC'),
        )
        create_table(get_covid_survey_covid_mask_2_table_name(self.test_prefix), table_schema)

    def output(self):
        return TableExists(get_covid_survey_covid_mask_2_table_name(self.test_prefix))


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
        run_query(
            f"DELETE FROM {get_covid_survey_covid_mask_2_table_name(self.test_prefix)} WHERE {self.get_sql_filter()}")
        # Insert data
        run_query(Template("""
            INSERT INTO {{table_name}}
            SELECT 
                    '{"covid:" ' || pct_covid || ', ' || '"mask": ' || percent_mc || '}'
                AS percent_json,
                    '{"covid": ' || covid_se || ', ' || '"mask": ' || mc_se || '}'
                AS json_se,
                    '{"covid": ' || pct_covid_unw || ', ' || '"mask": ' || percent_mc_unw || '}'
                AS percent_json_unw,
                    '{"covid": ' || covid_se_unw || ', ' || '"mask": ' || mc_se_unw || '}'
                AS json_se_unw,
                    '{"covid": ' || a.sample_size || ', ' || '"mask": ' || b.sample_size || '}'
                AS json_sample_size,
                a.country, 
                a.iso_code,
                a.gid_0,
                a.survey_date
            FROM rpl_covid_survey_covid a
            LEFT JOIN rpl_covid_survey_mask b
            ON a.survey_date = b.survey_date
            AND a.iso_code = b.iso_code
            WHERE
                a.survey_date = {{data_date}}
        """).render(
            table_name=get_covid_survey_covid_mask_2_table_name(self.test_prefix),
            data_date=self.date.replace('-', '')
        ),
            True
        )

    def output(self):
        return DataExists(table_name=get_covid_survey_covid_mask_2_table_name(self.test_prefix),
                          where_clause=self.get_sql_filter())

if __name__ == '__main__':
    luigi.build([LoadTable()], workers=5, local_scheduler=True)

# Scrip example to run the pipeline:
# python -m luigi --module pipelines.covid_survey_covid_mask_2 LoadTable --date 2021-07-01 --test-prefix test_0_ --local-scheduler
