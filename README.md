# Data Engineering Lab
---

## 1. Setup
---
To go through this lab you will need to install the following tools in your workstation.

### 1.A Set virtual environment using venv
If you are running this in your local machine, it's highly recommendable to use a virtual environment
to keep the version of your Python libraries under control (and avoid version conflicts).

You can install virtualenv using pip
> python3 -m pip install --user virtualenv

Create a virtual environment from your terminal running
> virtualenv {name_of_environment}

Then, activate the virtual environment running:
> -- Unix (Linux/Mac)
> source bin/activate
> -- Windows
> venv\Scripts\activate

Once you are inside the virtual environment, install dependencies:
> pip3 install -r requirements.txt

Sorted! Now let's begin by the Jupyter notebook. Run from the terminal:
> jupyter notebook
  

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


## 3. Repository content
---
Files you will find in this repository:
- Data Engineering Lab.ipynb: A Jupyter notebook with some examples of data manipulation that serves as an initial setup.
- lab_params.py: Several parameter we will use during the lab (database settings, file paths)
- lab_utils.py: A collection of functions to be reused in the lab, mostly to interact with the database (create tables, load data, run queries,retrieve data...).
- pipelines (folder): You can find here some examples of data pipelines using Luigi.
    - rpl_covid_survey.py : Downloads daily reports from API.
    - covid_survey_covid_mask.py : Joins 2 reports from different indicators (covid & mask) into a single table.
    - covid_survey_covid_mask_2.py: Similar to the previous pipeline, but using a table schema that is more escalable.
    - covid_survey_json.py: Yet another iteration on covid_survey_covid_mask, but now making it fully escalable to handle a dinamic list of rpl_covid_XXX reports as input.


# 4. Useful links
- [Facebook Data For Good](https://dataforgood.facebook.com/)
- [The Global COVID-19 Trends and Impact Survey Open Data API](https://gisumd.github.io/COVID-19-API-Documentation/)
- [Movement Range Maps](https://dataforgood.facebook.com/dfg/tools/movement-range-maps)
- [High Resolution Population Density Maps](https://dataforgood.facebook.com/dfg/tools/high-resolution-population-density-maps)
