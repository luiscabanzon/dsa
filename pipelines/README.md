# Hands-on: COVID Survey data pipelines with luigi

---

**NOTE**: To execute each of these pipelines, run on the terminal:
```commandline
python -m luigi --module pipelines.{pipeline_name} --date {YYYY-MM-DD} --test-prefix {optional_test_prefix} --local-scheduler

EXAMPLE: python -m luigi --module pipelines.covid_survey_covid_mask LoadTable --date 2021-07-01 --test-prefix test_1_ --local-scheduler
```


## 0. rpl_covid_survey.py
First things first. Before processing some data... We need to have some data at all!
This pipeline downloads on a daily basis different survey indicators for several countries.

> **Takeaway**: Automation allows us to process big amounts of data and run complex data transformations
> with no extra effort (so you can spend your time on higher-value work).

## 1. covid_survey_covid_mask.py

This pipeline joins 2 different indicators (covid & mask) in a single table.
Keeps the common granularity of both (date and country)
and the data from each indicator in separated columns.

Some limitations of this approach are:
1. if we keep adding indicators, the number of columns will keep growing
2. What if some indicator is discontinued (as it actually happened, some of them have been surveyed for a few months)? Those fields will get empty.

> **Takeaway**: Enriching data by crossing different sources (or pre-computing some extra information)
> makes it not only <u>**more useful**</u> but also <u>**reusable**</u>
> (you compute once, and use as many times in as many places as you want).

## 2. covid_survey_covid_mask_2.py

This is an iteration on the previous pipeline, but instead of stacking more columns per indicator,
we consolidate the same metrics from different indicators into a single field,
as a JSON-formatted string (some databases support JSON natively as a field time,
sometimes it is just stored as plain text and them parsed accordingly when data is consumed).


> **Takeaway**: **<u>Scalability</u>** is one of the main criteria when assessing
> **<u>data format </u>** (in this case the **<u>table schemas</u>**).

## 3. covid_survey_json.py

> **Takeaway**: **<u>Scalability</u>** is not only tackled through the data itself,
> but also through the code. Parametrisation allows to make the output adaptable as the input evolves.


# What now?

-----------

1. In its current version rpl_covid_survey.py only downloads data for 2 indexes and 2 countries... How can we add some more?
2. Let's say you want to run an analysis using other indicators from the survey... How can we include them covid_survey_json.py?

