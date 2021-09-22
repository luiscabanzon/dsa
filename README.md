# Data Engineering Lab
---

## 1. Setup up
---
To go through this lab you will need to install the following tools in your workstation.

### 1.A PostgreSQL
Download it from the [official website](https://www.postgresql.org/). It uncludes both the database (which we will use to store and process data) as well as *pgAdmin*, and environment to query and manage Postgre databases.

### 1.B Python 3 libraries
We need the following libraries:
- [Jupyter](https://jupyter.org/install): Needed to work with Jupyter notebooks.
- [Luigi](https://luigi.readthedocs.io/en/stable/index.html): This library allows to build and schedule data pipelines.

> ⚠️ To have access to all Luigi's feature you need a UNIX machine (Linux, macOS). In this lab we will just the pipelines using the local-scheduler mode.

- [Pandas](https://pandas.pydata.org/docs/getting_started/install.html): One of the best-known Python libraries for data manipulation.
- [SQL Alchemy](https://docs.sqlalchemy.org/en/14/intro.html): Library used as an interface to interact from Python with different databases.
- [Psycopg](https://www.psycopg.org/docs/install.html): This module serves as a connector to PostgreSQL.


## 2. Learn SQL
In this lab we use some basic **SQL** (Structured Query Language). This is the main tool to interact with the bast mayority of databases. Each SQL have its own "dialect", bust there is a common core to all of them.

If you are not familiar with SQL already have no worries, it's by far the easiest programming language. You can get a grasp of the basics in any of the resources below:
- [W3Schools](https://www.w3schools.com/sql/)
- [SQL Zoo](https://sqlzoo.net/wiki/SQL_Tutorial)
- [CodeCademy](https://www.codecademy.com/courses/learn-sql/)


## 2. Repository content
---
Files you will find in this repository:
- Data Engineering Lab.ipynb: A Jupyter notebook with an some examples of data manipulation that serves as an initial setup.
- lab_params.py: Several parameter we will use during the lab (database settings, file paths)
- lab_utils.py: A collection of functions to be reused in the lab, mostly to interact with the database (create tables, load data, run queries,retrieve data...).
- pipelines (folder): You can find here some example of data pipelines using Luigi.