import io
import json

import requests
from dotenv import load_dotenv
import os
import sqlite3
from datetime import datetime
from typing import List
import pandas as pd
import pyarrow.parquet as pq



load_dotenv()

connection = sqlite3.connect("db.db")
cursor = connection.cursor()

BASE_URL = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"
AUTHORIZATION_TOKEN = os.getenv("AUTHORIZATION")
DATE = "2020_01_01"


def drop_if_exists(table_name: str) -> None:
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    result = cursor.fetchone()
    if result:
        print("Dropping existing table")
        cursor.execute(f"DROP TABLE {table_name};")

def save_installs_to_database(data: dict) -> None:
    table_name = f"installs_{DATE}"
    drop_if_exists(table_name)
    create_table_command = f"""CREATE TABLE {table_name} (
        install_time DATETIME,
        marketing_id TEXT,
        channel TEXT,
        medium TEXT,
        campaign TEXT,
        keyword TEXT,
        ad_content TEXT,
        ad_group TEXT,
        landing_page TEXT,
        sex TEXT,
        alpha_2 TEXT,
        alpha_3 TEXT,
        flag TEXT,
        country_name TEXT,
        country_numeric TEXT,
        official_name TEXT
    );"""
    cursor.execute(create_table_command)
    for record in data["records"]:
        install_time = datetime.strptime(record["install_time"], "%Y-%m-%dT%H:%M:%S.%f")
        marketing_id = record["marketing_id"]
        channel = record["channel"]
        medium = record["medium"]
        campaign = record["campaign"]
        keyword = record["keyword"]
        ad_content = record["ad_content"]
        ad_group = record["ad_group"]
        landing_page = record["landing_page"]
        sex = record["sex"]
        alpha_2 = record["alpha_2"]
        alpha_3 = record["alpha_3"]
        flag = record["flag"]
        country_name = record["name"]
        country_numeric = record["numeric"]
        official_name = record["official_name"]

        cursor.execute(f"""INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (install_time, marketing_id, channel, medium, campaign, keyword, ad_content, ad_group,
                   landing_page, sex, alpha_2, alpha_3, flag, country_name, country_numeric, official_name))

    connection.commit()


def fetch_installs_data_from_api() -> dict:
    response = requests.get(BASE_URL + f"installs?date={DATE.replace('_', '-')}", headers={"Authorization": AUTHORIZATION_TOKEN})
    data = response.json()
    data["records"] = json.loads(data["records"])
    return data


def get_installs_table():
    print("Fetching installs data")
    data = fetch_installs_data_from_api()
    print("Saving installs data to DB")
    save_installs_to_database(data)
    print("Successfully saved installs data to DB")


def save_costs_to_database(data: List[str]) -> None:
    table_name = f"costs_{DATE}"
    drop_if_exists(table_name)

    create_table_command = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        campaign TEXT,
        location TEXT,
        ad_group TEXT,
        ad_content TEXT,
        keyword TEXT,
        landing_page TEXT,
        medium TEXT,
        channel TEXT,
        cost DECIMAL(10, 3)
    );"""
    cursor.execute(create_table_command)

    for row in data:
        if row.strip():
            cursor.execute(f"""INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", row.split(sep="\t"))
    connection.commit()



def fetch_costs_data_from_api() -> List[str]:
    response = requests.get(BASE_URL + f"costs?date={DATE.replace('_', '-')}&dimensions=location,campaign,channel,medium,keyword,ad_content,ad_group,landing_page", headers={"Authorization": AUTHORIZATION_TOKEN})
    _, data = response.text.split(sep="\n", maxsplit=1)
    data = data.split(sep="\n")
    return data


def get_costs_table() -> None:
    print("Fetching costs data")
    data = fetch_costs_data_from_api()
    print("Saving costs data to DB")
    save_costs_to_database(data)
    print("Successfully saved costs data to DB")

def get_events_table() -> None:
    print("Fetching events data")
    data = fetch_events_data_from_api()
    print("Saving events data to DB")
    save_events_to_database(data)
    print("Successfully saved events data to DB")

def save_events_to_database(data: List[dict]) -> None:
    user_params_table_name = f"user_params_{DATE}"
    drop_if_exists(user_params_table_name)

    create_table_command = f"""CREATE TABLE IF NOT EXISTS {user_params_table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        os TEXT,
        brand TEXT,
        model TEXT,
        model_number REAL,
        specification TEXT,
        transaction_id TEXT NULL,
        campaign_name TEXT NULL,
        source TEXT NULL,
        medium TEXT NULL,
        term TEXT NULL,
        context TEXT NULL,
        gclid TEXT NULL,
        dclid TEXT NULL,
        srsltid TEXT NULL,
        is_active_user TEXT NULL,
        marketing_id TEXT
    );"""
    cursor.execute(create_table_command)

    events_table_name = f"events_{DATE}"
    drop_if_exists(events_table_name)

    create_table_command = f"""CREATE TABLE IF NOT EXISTS {events_table_name} (
        user_id TEXT,
        alpha_2 TEXT,
        alpha_3 TEXT,
        flag TEXT,
        country_name TEXT,
        country_numeric TEXT,
        official_name TEXT NULL,
        os TEXT,
        brand TEXT,
        model TEXT,
        model_number REAL,
        specification TEXT,
        event_time TIME,
        event_type TEXT,
        location TEXT NULL,
        user_action_detail TEXT,
        session_number TEXT NULL,
        localization_id TEXT,
        ga_session_id TEXT,
        value REAL,
        state REAL,
        engagement_time_msec REAL,
        current_progress TEXT NULL,
        event_origin TEXT,
        place REAL,
        selection TEXT NULL,
        analytics_storage TEXT,
        browser TEXT NULL,
        install_store TEXT NULL,
        user_params INTEGER NULL,
        FOREIGN KEY (user_params) REFERENCES {user_params_table_name}(id)
    );"""
    cursor.execute(create_table_command)

    for row in data:
        user_params = row.get("user_params")
        last_id = None

        if user_params:
            os = user_params["os"]
            brand = user_params["brand"]
            model = user_params["model"]
            model_number = user_params["model_number"]
            specification = user_params["specification"]
            transaction_id = user_params["transaction_id"]
            campaign_name = user_params["campaign_name"]
            source = user_params["source"]
            medium = user_params["medium"]
            term = user_params["term"]
            context = user_params["context"]
            gclid = user_params["gclid"]
            dclid = user_params["dclid"]
            srsltid = user_params["srsltid"]
            is_active_user = user_params["is_active_user"]
            marketing_id = user_params["marketing_id"]

            cursor.execute(
                f"""INSERT INTO {user_params_table_name}
                (os, brand, model, model_number, specification, transaction_id,
                campaign_name, source, medium, term, context, gclid, dclid,
                srsltid, is_active_user, marketing_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (os, brand, model, model_number, specification, transaction_id,
                campaign_name, source, medium, term, context, gclid, dclid,
                srsltid, is_active_user, marketing_id)
            )
            cursor.execute(f"SELECT MAX(id) FROM {user_params_table_name}")
            last_id = cursor.fetchone()[0]

        user_id = row["user_id"]
        alpha_2 = row["alpha_2"]
        alpha_3 = row["alpha_3"]
        flag = row["flag"]
        country_name = row["name"]
        country_numeric = row["numeric"]
        official_name = row.get("official_name", None)
        operational_system = row["os"]
        brand = row["brand"]
        model = row["model"]
        model_number = row["model_number"]
        specification = row["specification"]
        event_type = row["event_type"]
        location = row["location"]
        user_action_detail = row["user_action_detail"]
        session_number = row["session_number"]
        localization_id = row["localization_id"]
        ga_session_id = row["ga_session_id"]
        value = row["value"]
        state = row["state"]
        engagement_time_msec = row["engagement_time_msec"]
        current_progress = row["current_progress"]
        event_origin = row["event_origin"]
        place = row["place"]
        selection = row["selection"]
        analytics_storage = row["analytics_storage"]
        browser = row["browser"]
        install_store = row["install_store"]
        event_time = datetime.fromtimestamp(
            row["event_time"] / 1000
        ).strftime("%H:%M:%S")

        cursor.execute(
            f"""INSERT INTO {events_table_name} VALUES (?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, alpha_2, alpha_3, flag, country_name, country_numeric, official_name, operational_system, brand, model, model_number, specification, event_time, event_type, location, user_action_detail, session_number, localization_id, ga_session_id, value, state, engagement_time_msec, current_progress, event_origin, place, selection, analytics_storage, browser, install_store, last_id)
        )

    connection.commit()


def fetch_events_data_from_api(next_page: str = "") -> List:
    core_page = BASE_URL + f"events?date={DATE.replace('_', '-')}"

    response = requests.get(core_page + next_page, headers={"Authorization": AUTHORIZATION_TOKEN})
    while response.text == "Error":
        response = requests.get(core_page + next_page, headers={"Authorization": AUTHORIZATION_TOKEN})

    data = response.json()
    next_page = data.get("next_page")
    next_page_data = None
    if next_page:
        next_page_data = fetch_events_data_from_api(f"&next_page={next_page}")

    data["data"] = json.loads(data["data"])

    if next_page_data:
        data["data"] += next_page_data

    return data["data"]


def fetch_orders_data_from_api() -> pd.DataFrame:
    response = requests.get(BASE_URL + f"orders?date={DATE.replace('_', '-')}",
                            headers={"Authorization": AUTHORIZATION_TOKEN})

    parquet_content = response.content
    parquet_file = io.BytesIO(parquet_content)
    parquet_table = pq.read_table(parquet_file)

    return parquet_table.to_pandas()


def save_orders_to_database(df: pd.DataFrame) -> None:
    table_name = f"orders_{DATE}"
    df.to_sql(name=table_name, con=connection, if_exists='replace', index=False)


def get_orders_table() -> None:
    print("Fetching orders data")
    df = fetch_orders_data_from_api()
    print("Saving orders data to DB")
    save_orders_to_database(df)
    print("Successfully saved orders data to DB")


# COSTS
get_costs_table()
# INSTALLS
get_installs_table()
# EVENTS
get_events_table()
# ORDERS
get_orders_table()

connection.close()
