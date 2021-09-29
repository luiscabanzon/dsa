# Data Engineering Lab
---

## 1. Setup up
---
To go through this lab you will need to install the following tools in your workstation.

### 1.A PostgreSQL
Download it from the [official website](https://www.postgresql.org/). It uncludes both the database (which we will use to store and process data) as well as *pgAdmin*, and an environment to query and manage Postgre databases.
During installation keep note of 2 things we will need to connect to the database afterwards:
- 1. Port: By default it should be 5432. If it's any other, keep note of it
- 2. Superuser's (postgres) password: To keep it simple I set it to "postgres", but if you set any other... Don't forget it!

### 1.B Python 3 libraries
We need the following libraries:
- [Jupyter](https://jupyter.org/install): Needed to work with Jupyter notebooks.
- [Luigi](https://luigi.readthedocs.io/en/stable/index.html): This library allows us to build and schedule data pipelines.

> ⚠️ To have access to all Luigi's features you need a UNIX machine (Linux, macOS). In this lab we will just run the pipelines using the local-scheduler mode.

- [Pandas](https://pandas.pydata.org/docs/getting_started/install.html): One of the best-known Python libraries for data manipulation.
- [SQL Alchemy](https://docs.sqlalchemy.org/en/14/intro.html): Library used as an interface to interact from Python with different databases.
- [Psycopg](https://www.psycopg.org/docs/install.html): This module serves as a connector to PostgreSQL.

If you have pip installed, you can download these libraries using the requirements file in this repository as follows:
> pip install -r requirements.txt

## 2. Learn SQL
In this lab we use some basic **SQL** (Structured Query Language). This is the main tool to interact with the vast majority of databases. Each SQL has its own "dialect", but there is a common core to all of them.

If you are not familiar with SQL already have no worries, it's by far the easiest programming language. You can get a grasp of the basics in any of the resources below:
- [Mode](https://mode.com/sql-tutorial/introduction-to-sql/)
- [W3Schools](https://www.w3schools.com/sql/)
- [SQL Zoo](https://sqlzoo.net/wiki/SQL_Tutorial)
- [CodeCademy](https://www.codecademy.com/courses/learn-sql/)


## 2. Repository content
---
Files you will find in this repository:
- Data Engineering Lab.ipynb: A Jupyter notebook with some examples of data manipulation that serves as an initial setup.
- lab_params.py: Several parameter we will use during the lab (database settings, file paths)
- lab_utils.py: A collection of functions to be reused in the lab, mostly to interact with the database (create tables, load data, run queries,retrieve data...).
- pipelines (folder): You can find here some examples of data pipelines using Luigi.
    - rpl_covid_survey.py : Downloads daily reports from API.
    - covid_survey_covid_mask.py : Joins 2 reports from different indicators (covid & mask) into a single table.
    - covid_survey_covid_mask_2.py: Similar to the previous pipeline, but using a table schema that is more escalable.
