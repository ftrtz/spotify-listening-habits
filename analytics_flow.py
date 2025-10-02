from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from jinja2 import Template

PROD_SCHEMA="prod"
ANALYTICS_SCHEMA="stats"



@task
def reset_stats_tables():
    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        with open("sql/reset_stats_tables.sql", "r") as file:
            sql = Template(file.read()).render(prod_schema=PROD_SCHEMA, analytics_schema=ANALYTICS_SCHEMA)
            db.execute(sql)


@task
def calc_artist_monthly():
    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        with open("sql/calc_artist_monthly.sql", "r") as file:
            sql = Template(file.read()).render(prod_schema=PROD_SCHEMA, analytics_schema=ANALYTICS_SCHEMA)
            db.execute(sql)


@flow()
def analytics_flow():
    reset_stats_tables()
    calc_artist_monthly()

if __name__ == "__main__":
    analytics_flow()
