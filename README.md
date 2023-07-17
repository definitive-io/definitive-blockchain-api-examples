# Definitive API Boilerplate

These are boilerplate Python scripts which can be used to directly call the Definitive advisor endpoints.
Current functionality:

- Execute sql query
- Poll results of query job into dataframe

This boilerplate will expand as we determine use cases for additional endpoints.

Swagger docs

https://public.advisor.definitive.io/v0/docs#/

---

# Quick Start

In order to make use of Definitive's advisor APIs, you will need to do the following

1. Have Definitive provision an API key for access to the advisor API
2. Using definitive.io, write prompts to generate SQL
3. Analyze prompt results and modify prompt (or SQL) as required to get to a desired SQL output
4. Refit generated SQL to be included in an ETL pipeline (ie. date incremental)
5. Place SQL in sql/ in such a way that an ETL process can run the script
6. Verify SQL runs calling the endpoint

Using this as a starting point, you should be able to quickly spin up an ETL pipeline sourced through Definitves adivsor API!

---

# Executing Python Scripts to Call Endpoints

First, you will want to install packages require for local execution of the sample python scripts. This can be accmomplished by running.

```bash
pip3 install -r requirements.txt
```

This requirements file is required to run the sample workflows provided in this repo. Developers will be responsible for augmenting this for their development needs.

This sample can be executed by running

```bash
python3 execute_sql_api.py
```

# Suggested Solution Patterns

Execute sql and read results: 

- Leverage a scheduling tool to run the execute_sql_api script (cron, airflow, etc.)
- Store desired SQL files in directories in a way that is conducive a scheduling tool
(ie. dirs: hourly, daily, weekly, monthly, etc.)
- Design SQL to match a pattern based on the schedule the model follows
(ie. daily -> select * from table where date_key = execution_date)
- Create a target destination to write the data coming from the API (warehouse/DB of choice)
- Dataframes are a good method of loading the data into the destination given pandas flexibility

# Additional Considerations

The returned results are limited to 20000 rows within the dataframe.
