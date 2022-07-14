# Hands-on: COVID Survey data pipelines with luigi

---


## 0. rpl_covid_survey.py
First things first. Before processing some data... We need to have some data at all!
This pipeline downloads on a daily basis different survey indicators for several countries.

> Takeaway: Automation allows us to process big amounts of data and run complex data transformations
> with no extra effort (so you can spend your time on higher-value work).

## 1. covid_survey_covid_mask.py

This pipeline joins 2 different indicators (covid & mask) in a single table.

> Takeaway: Enriching data by crossing different sources (or pre-computing some extra information)
> makes it not only <u>**more useful**</u> but also <u>**reusable**</u>
> (you compute once, and use as many times in as many places as you want).

## 2. covid_survey_covid_mask.py

> Takeaway: 

## 3. covid_survey_json.py

> Takeaway:


# What now?

-----------

1. In its current version rpl_covid_survey.py only downloads data for 2 indexes and 2 countries...Could you add some more?
2. Let's say you want to run an analysis using other indicators from the survey... How would you include them covid_survey_json.py?

