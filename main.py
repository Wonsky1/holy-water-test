import io
import json
import requests
import os
import base64
import datetime
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()


conn_string = os.getenv("CONN_STRING")
db = create_engine(conn_string)
connection = db.connect()


BASE_URL = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"
AUTHORIZATION_TOKEN = os.getenv("AUTHORIZATION")

SCHEDULE_TIME = "17:53"


def fetch_installs_data_from_api(date: str) -> pd.DataFrame:
    response = requests.get(
        BASE_URL + f"installs?date={date.replace('_', '-')}",
        headers={"Authorization": AUTHORIZATION_TOKEN},
    )
    data = response.json()
    data = json.loads(data["records"])
    df = pd.DataFrame(data)
    return df


def fetch_costs_data_from_api(date: str) -> pd.DataFrame:
    response = requests.get(
        BASE_URL + f"costs?date={date.replace('_', '-')}"
        f"&dimensions=location,campaign,channel,medium,"
        f"keyword,ad_content,ad_group,landing_page",
        headers={"Authorization": AUTHORIZATION_TOKEN},
    )
    rows = response.content.split(b"\n")
    columns = rows[0].split(b"\t")

    data = []

    if rows[-1] == b"":
        rows = rows[:-1]

    for row in rows[1:]:
        values = row.split(b"\t")
        row_dict = {
            column.decode(): value.decode()
            for column, value in zip(columns, values)
        }
        data.append(row_dict)

    df = pd.DataFrame(data)

    return df


def fetch_events_data_from_api(date: str, next_page: str = "") -> pd.DataFrame:
    core_page = BASE_URL + f"events?date={date.replace('_', '-')}"

    response = requests.get(
        core_page + next_page, headers={"Authorization": AUTHORIZATION_TOKEN}
    )
    while response.text == "Error":
        response = requests.get(
            core_page + next_page,
            headers={"Authorization": AUTHORIZATION_TOKEN},
        )

    data = response.json()
    next_page = data.get("next_page")
    next_page_df = pd.DataFrame()
    if next_page:
        next_page_df = fetch_events_data_from_api(
            date, f"&next_page={next_page}"
        )

    data["data"] = json.loads(data["data"])
    df = pd.DataFrame(data["data"])
    if not next_page_df.empty and next_page:
        df = pd.concat([df, next_page_df])
    return df


def fetch_orders_data_from_api(date: str) -> pd.DataFrame:
    response = requests.get(
        BASE_URL + f"orders?date={date.replace('_', '-')}",
        headers={"Authorization": AUTHORIZATION_TOKEN},
    )

    parquet_content = response.content
    parquet_file = io.BytesIO(parquet_content)
    parquet_table = pq.read_table(parquet_file)

    return parquet_table.to_pandas()


def save_tables_to_database(df: pd.DataFrame, table_names: tuple) -> None:
    if len(table_names) == 2:
        user_params_df = pd.json_normalize(df["user_params"])

        df.drop(columns=["user_params"], inplace=True)

        df["user_params"] = range(1, len(df) + 1)
        user_params_df.to_sql(
            name=table_names[1],
            con=connection,
            if_exists="replace",
            index=False,
        )
    df.to_sql(
        name=table_names[0], con=connection, if_exists="replace", index=False
    )


def get_all_tables() -> None:
    print("Starting fetching all the data")
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y_%m_%d"
    )

    print(date)

    # ORDERS
    print("Fetching orders data")
    df = fetch_orders_data_from_api(date)
    print("Saving orders data to DB")
    save_tables_to_database(df, (f"orders_{date}",))
    print("Successfully saved orders data to DB")

    # COSTS
    print("Fetching costs data")
    df = fetch_costs_data_from_api(date)
    print("Saving costs data to DB")
    save_tables_to_database(df, (f"costs_{date}",))
    print("Successfully saved costs data to DB")

    # INSTALLS
    print("Fetching installs data")
    df = fetch_installs_data_from_api(date)
    print("Saving installs data to DB")
    save_tables_to_database(df, (f"installs_{date}",))
    print("Successfully saved costs data to DB")

    # EVENTS
    print("Fetching events data")
    df = fetch_events_data_from_api(date)
    print("Saving events data to DB")
    save_tables_to_database(df, (f"events_{date}", f"user_params_{date}"))
    print("Successfully saved events data to DB")


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    get_all_tables()


if __name__ == "__main__":
    hello_pubsub("data", "context")
