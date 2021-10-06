#!/usr/bin/env python

import os

# DB config
DB_HOST='localhost'
DB_NAME='postgres'
DB_USER='postgres'
DB_PASSWORD='postgres'
DB_PORT=5432 # Default is 5432, if some service was using that port already then PostgreSQL's port may be 5433 instead

# File paths
RAW_DATA_FOLDER_PATH = os.path.join('data', 'raw')
MOVE_RANGE_FILE_PATH = os.path.join(RAW_DATA_FOLDER_PATH ,'movement-range-2021-09-09.txt')
COUNTRY_REGION_FILE_PATH = os.path.join(MOVE_RANGE_FILE_PATH , 'country_regions.txt')

# Table names
TEST_PREFIX=''
MOVE_RANGE_TABLE_NAME = f'{TEST_PREFIX}rpl_move_range'
COVID_SURVEY_TABLE_NAME = f'{TEST_PREFIX}rpl_covid_survey'

# Time range (for COVID survey API)
FROM_DATE = '2021-01-01'
TO_DATE = '2021-06-30'

# Other parameters
SURVEY_INDICATORS = [
    'mask',
    'covid',
    'tested_positive_14d',
    'anosmia',
    'covid_vaccine',
]