import io
import json
from typing import Literal, List, Tuple

import requests
import os
import datetime
import pandas as pd
import pyarrow.parquet as pq
import schedule
from sqlalchemy import create_engine, Connection
from dotenv import load_dotenv


load_dotenv()


conn_string = os.getenv("CONN_STRING")
db = create_engine(conn_string)
connection = db.connect()


BASE_URL = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"
AUTHORIZATION_TOKEN = os.getenv("AUTHORIZATION")

SCHEDULE_TIME = "10:00"


def get_response_without_error(
    url: str, headers: dict = {"Authorization": AUTHORIZATION_TOKEN}
) -> requests.Response:
    response = requests.get(url=url, headers=headers)
    while response.text == "Error":
        print(response.text)
        response = requests.get(url=url, headers=headers)

    return response


def fetch_installs_data_from_api(date: str) -> pd.DataFrame:
    response = get_response_without_error(
        BASE_URL + f"installs?date={date.replace('_', '-')}",
    )
    data = response.json()
    data = json.loads(data["records"])
    df = pd.DataFrame(data)
    return df


def fetch_costs_data_from_api(date: str) -> pd.DataFrame:
    response = get_response_without_error(
        BASE_URL + f"costs?date={date.replace('_', '-')}"
        f"&dimensions=location,campaign,channel,medium,"
        f"keyword,ad_content,ad_group,landing_page"
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
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")

    return df


def fetch_events_data_from_api(date: str, next_page: str = "") -> pd.DataFrame:
    core_page = BASE_URL + f"events?date={date.replace('_', '-')}"

    response = get_response_without_error(core_page + next_page)

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
    response = get_response_without_error(
        BASE_URL + f"orders?date={date.replace('_', '-')}",
    )

    parquet_content = response.content
    parquet_file = io.BytesIO(parquet_content)
    parquet_table = pq.read_table(parquet_file)

    return parquet_table.to_pandas()


def save_table_to_database(
    df: pd.DataFrame,
    name: str,
    con: Connection = connection,
    if_exists: Literal["fail", "replace", "append"] = "replace",
    index: bool = False,
) -> None:
    df.to_sql(
        name=name,
        con=con,
        if_exists=if_exists,
        index=index,
    )


def get_cpi_data_frame(
    costs_df: pd.DataFrame, installs_df: pd.DataFrame
) -> List[pd.DataFrame]:

    dataframes = []
    for field in [
        "medium",
        "ad_group",
        "channel",
        "campaign",
        "landing_page",
        "keyword",
        "ad_content",
        "location",
    ]:
        if field == "location":
            by_field_right = "alpha_2"
            costs_df.loc[costs_df["location"] == "UK", "location"] = "GB"
        else:
            by_field_right = field
        counts = installs_df[by_field_right].value_counts()

        values_sum = []
        for value, count in counts.items():
            values_sum.append(costs_df[costs_df[field] == value]["cost"].sum())
        result = counts.to_frame()
        result["total_amount_spent"] = values_sum
        result["cpi"] = values_sum / result["count"]
        result.rename(columns={"count": "installs_count"}, inplace=True)
        result[field] = result.index
        dataframes.append(result)

    return dataframes


def get_events_and_user_params_frames(
    events_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    user_params_df = pd.json_normalize(events_df["user_params"])

    events_df.drop(columns=["user_params"], inplace=True)
    events_df["user_params"] = range(1, len(events_df) + 1)

    return events_df, user_params_df


def get_arpu_and_roas_frames(
    event_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    costs_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    unique_users_count = event_df["user_id"].nunique()

    total_revenue = (
        orders_df["iap_item.price"]
        - orders_df["discount.amount"]
        - orders_df["tax"]
        - orders_df["fee"]
    ).sum()

    arpu = unique_users_count / total_revenue

    arpu_df = pd.DataFrame(
        {
            "unique_users_count": [unique_users_count],
            "total_revenue": [total_revenue],
            "arpu": [arpu],
        }
    )

    amount_spent = costs_df["cost"].sum()
    roas = (total_revenue / amount_spent) * 100

    roas_df = pd.DataFrame(
        {
            "total_revenue": [total_revenue],
            "amount_spent": [amount_spent],
            "roas": [roas],
        }
    )

    return arpu_df, roas_df


def get_all_tables() -> None:
    print("Starting fetching all the data")
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y_%m_%d"
    )

    print(date)

    # INSTALLS
    print("Fetching installs data")
    installs_df = fetch_installs_data_from_api(date)
    print("Saving installs data to DB")
    save_table_to_database(installs_df, f"installs_{date}")
    print("Successfully saved installs data to DB")

    # COSTS
    print("Fetching costs data")
    costs_df = fetch_costs_data_from_api(date)

    print("Saving costs data to DB")
    cpi_dfs = get_cpi_data_frame(costs_df, installs_df)
    for cpi_df in cpi_dfs:
        save_table_to_database(
            cpi_df, f"cpi_{date}_{cpi_df.columns[-1]}", connection
        )

    save_table_to_database(costs_df, f"costs_{date}")
    print("Successfully saved costs data to DB")

    # EVENTS
    print("Fetching events data")
    events_df = fetch_events_data_from_api(date)
    print("Saving events data to DB")
    events_df, user_params_df = get_events_and_user_params_frames(events_df)
    save_table_to_database(events_df, f"events_{date}")
    save_table_to_database(user_params_df, f"events_{date}")
    print("Successfully saved events data to DB")

    # ORDERS
    print("Fetching orders data")
    orders_df = fetch_orders_data_from_api(date)
    print("Saving orders data to DB")
    arpu_df, roas_df = get_arpu_and_roas_frames(events_df, orders_df, costs_df)
    save_table_to_database(orders_df, f"orders_{date}")
    save_table_to_database(arpu_df, f"arpu_{date}")
    save_table_to_database(roas_df, f"roas_{date}")
    print("Successfully saved orders data to DB")


if __name__ == "__main__":
    try:
        schedule.every().day.at(SCHEDULE_TIME).do(get_all_tables)
        while True:
            schedule.run_pending()
    finally:
        connection.close()
