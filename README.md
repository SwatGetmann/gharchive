# GitHub Archive - Test Exercise

## How to start?

1. Use or create a new .env file in ./docker directory.

Important variables:
- ENV - tag for containers, 'dev' / 'staging' / 'production'
- AIRFLOW_UID
- CLICKHOUSE_DB - database to use for clickhouse, 'gharchive' by default
- CLICKHOUSE_USER - username for CH
- CLICKHOUSE_PASSWORD - password for CH
- JUPYTER_TOKEN - token to enter jupyter lab node

2. Run Docker Compose

> docker compose up -d

## Available services

### Airflow
GHArchive (GHA) tagged dags - http://localhost:8080/home?tags=gharchive

DAGS:

1. __Daily GHA Crawler DAG__ - http://localhost:8080/dags/gharchive_01_crawl_daily/grid \
Runs once each day. \
Collects previous day hourly datasets from GHArchive. \
Triggers Spark DAG, passing the date, for wihch the crawling had been made.\
__NOTE__: catchup is set to True, start date is 2023-07-26, to get at least last 5 days worth of data.

2. __Spark DAG__ - http://localhost:8080/dags/gharchive_02_spark/grid \
Runs on demand. By default is triggered by Daily GHA Crawler DAG. \
Required parameter: "date", "YYYY-MM-DD" format. \
Runs a spark job (single core variant), that extracts & aggregates data from previous step, then loads results into Clickhouse. \
In the end, after handling all hourly datasets, DAG removes those files. \
__NOTE__: There is a multicore variant for this DAG - http://localhost:8080/dags/gharchive_02b_spark_multicore/grid. \
It's not enabled by default (due to limitations on my machine), but it can be helpful, if you have several Spark workers and several cores on one worker node.

3. __Metrics DAG__ - http://localhost:8080/dags/gharchive_03_metrics_daily/grid \
Runs daily. \
Queries Clickhouse to get metrics and saves results into ./data directory as CSV files. \
Querying logic is the same as in Dash dashboard, but in Dash there is a limit (100 top records) and here there are no limits, so the dataset is complete.


### Clickhouse
Available via ports 9000, 8123.

Paly mode: http://localhost:8123/play

Native client via docker:
> docker exec -it clickhouse-server clickhouse-client

### Jupyter Lab 
http://localhost:8888/lab

Has notebooks that were used for prototyping the solutions (Dash, clickhouse, metrics, spark job).

You can treat it as ~kind of staging...~ :)

### Spark
UI: http://localhost:8181/

There is a main node, and only one worker node (1 core, 1gb) enabled by default. \
You can tweak those parameters in docker-compose as you like. \
Due to my machine limitations, I was unable to run more than 1g single-core Spark worker.

### Dash 
UI: http://localhost:8051/ 

The service is used as a realtime dashboarding solution for the required metrics.

P.S. And if you run Jupyter Notebook [DASH.ipynb](http://localhost:8888/lab/tree/work/notebooks/DASH.ipynb), you'll also have a Dash app server running at http://localhost:8050/ with the same dashboard.

## Notes

1. One of the metrics was skipped due to inability to find the data for it. \
The metric is 'Total Developers grouped by gender'. \
In last year events with which I've worked, there were not fields associated with gender / sex identification. \
The only possible way I came up with was to collect all user names and crawl their pronouns from GH profiles. But even that does not guarantee full coverage of gender information, as pronouns are optional. \
That 'workaround' required too much chore for me, so i've skipped it.

2. Another metrics were little bit misunderstood until the final day of working on the solution. \
Those are 'commit'-based metrics: 
- List of Developers who did more than one commit in a day, ordered by name and number of commits;
- List of Developers with less than one commit in a day; \
I have built similar metrics, but instrad of commits, I've provided the global PushEvent statistics. \
To convert it into commits we just need to explode all the commits and their dt info from PushEvent payloads.

3. Overall docker contaier setup was and is heavily affecting my old machine with ~100 GB free disk space and ~8GB RAM. \
So I had to put severe limitations, both on how many datasets I can process (5 days at max) and how may Spark workers there are (1 worker, 1gb ram, 1 core). \
If you have strogner host, you can definetely try to tweak those configurations and add more Spark nodes, as well as go deeper than the last week for GHA datasets.
