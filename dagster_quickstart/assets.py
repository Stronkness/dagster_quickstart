from polars import read_csv, Series, mean
from dagster import asset
import pandas as pd
from datetime import datetime
import numpy as np


def get_df():
    return read_csv("/home/microcookies/dagster_quickstart/dagster_quickstart/people-2000000.csv")


@asset
def people_dataframe():
    return get_df()


@asset(deps=[people_dataframe])
def unique_firstnames():
    return get_df()["First Name"].unique()


@asset(deps=[people_dataframe])
def middle_age_per_jobtitle():
    df = get_df()

    # Convert date of birth to ages
    datetime_now = datetime.now().strftime("%Y-%m-%d")
    datetime_now = pd.to_datetime(datetime_now)
    datetime_idx = pd.to_datetime(df['Date of birth'])
    age_years = np.floor((datetime_now - datetime_idx) /
                         pd.Timedelta(days=365))
    age_series = Series("Age", age_years)
    df.insert_column(df.width, age_series)

    # Calculate mean age per job title
    mean_age_per_title = df.groupby('Job Title').agg(
        mean('Age').alias('Mean Age'))

    return mean_age_per_title


@asset(deps=[people_dataframe])
def change_lastname_anonymously():
    df = get_df()
    shifted_last_names = df["Last Name"].shift(-1)
    last_index = len(shifted_last_names) - 1
    shifted_last_names[last_index] = df["Last Name"][0]
    df.replace_column(df.get_column_index("Last Name"), shifted_last_names)
    return df
