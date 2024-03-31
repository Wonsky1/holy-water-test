import base64
import re
from typing import List, Optional

import os
import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData
from dotenv import load_dotenv

load_dotenv()


db = create_engine(os.getenv("CONN_STRING"))
connection = db.connect()

SCHEDULE_TIME = "10:00"


def get_available_tables() -> list:
    metadata = MetaData()
    metadata.reflect(bind=db)
    return list(metadata.tables.keys())


def get_tables_by_name(name: str, tables: list) -> list:
    return [table for table in tables if name in table]


def get_weekly_table_names(tables_by_name: list) -> list:
    dates = []
    today = datetime.datetime.today()
    for i in range(1, 8):
        date = today - datetime.timedelta(days=i)
        formatted_date = date.strftime("%Y_%m_%d")
        dates.append(formatted_date)

    weekly_table_names = [
        table_name
        for table_name in tables_by_name
        if any(date in table_name for date in dates)
        and "_to_" not in table_name
    ]

    return weekly_table_names


def get_date(table_name: str) -> str:
    re_pattern = r"(?<=_)\d{4}_\d{2}_\d{2}"
    match = re.search(re_pattern, table_name)
    return match.group(0)


def create_combined_dataframe(table_names: List[str]) -> pd.DataFrame:
    df = pd.DataFrame()
    for table in table_names:
        date = get_date(table)
        new_df = pd.read_sql_table(table, connection)
        new_df["date"] = date
        df = pd.concat([df, new_df], ignore_index=True)
    return df


def save_table(
    first_date: str,
    last_date: str,
    df: pd.DataFrame,
    prefix: str,
    group: Optional[str] = None,
) -> None:
    name = f"{prefix}_{first_date}_to_{last_date}"
    if group:
        name += f"_{group}"
    df.to_sql(
        name=name,
        con=connection,
        if_exists="replace",
        index=False,
    )


def save_result_tables(table_names, prefix: str) -> None:
    if prefix == "cpi":
        groups = [
            "ad_content",
            "ad_group",
            "campaign",
            "channel",
            "keyword",
            "landing_page",
            "location",
            "medium",
        ]
        for group in groups:
            tables = []
            for index in range(len(table_names) - 1, -1, -1):
                if group in table_names[index]:
                    tables.append(table_names[index])
            tables.sort()

            df = create_combined_dataframe(tables)

            save_table(
                get_date(tables[0]), get_date(tables[-1]), df, prefix, group
            )
    else:
        tables = table_names
        tables.sort()

        df = create_combined_dataframe(tables)

        save_table(get_date(tables[0]), get_date(tables[-1]), df, prefix)


def get_all_tables() -> None:
    for name in ["roas", "arpu", "cpi"]:
        print(f"Saving {name} tables")
        all_tables = get_available_tables()
        tables_by_name = get_tables_by_name(name, all_tables)
        weekly_table_names = get_weekly_table_names(tables_by_name)
        save_result_tables(weekly_table_names, name)
    print("Successfully saved all tables")


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    print(pubsub_message)
    get_all_tables()


if __name__ == "__main__":
    hello_pubsub("data", "context")
