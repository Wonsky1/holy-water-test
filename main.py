import json

import requests
from dotenv import load_dotenv
import os
import sqlite3
from datetime import datetime


load_dotenv()

connection = sqlite3.connect("db.db")
cursor = connection.cursor()

BASE_URL = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"
AUTHORIZATION_TOKEN = os.getenv("AUTHORIZATION")


def save_installs_to_database(data: dict) -> None:
    sql_command = """CREATE TABLE IF NOT EXISTS installs (
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
    cursor.execute(sql_command)
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

        cursor.execute('''INSERT INTO installs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (install_time, marketing_id, channel, medium, campaign, keyword, ad_content, ad_group,
                   landing_page, sex, alpha_2, alpha_3, flag, country_name, country_numeric, official_name))

    connection.commit()
    connection.close()


def fetch_installs_data_from_api() -> dict:
    response = requests.get(BASE_URL + "installs?date=2020-01-01", headers={"Authorization": AUTHORIZATION_TOKEN})
    data = response.json()
    records_str = data["records"]
    data["records"] = json.loads(records_str)
    return data


def get_installs_table():
    data = fetch_installs_data_from_api()
    print(data["count"])
    print(data["records"][-1]["marketing_id"])
    save_installs_to_database(data)


get_installs_table()
