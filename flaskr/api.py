import math

from sqlalchemy import text
from flask import Flask, request

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

app = Flask(__name__)


def list_to_matrix(list):
    matrix = []

    for i in range(0, len(list), 10):
        matrix.append(list[i:i+10])

    return matrix

def get_data_between_dates(start_date, end_date):
    statement = text('''
         WITH
         ids_in_dates AS
         (
         SELECT *, ROW_NUMBER () OVER (PARTITION BY id ORDER BY lut DESC) AS rank_number
         FROM event
         WHERE lut BETWEEN :start AND :end)
         SELECT id, lut, name, longitude, latitude, description
         FROM ids_in_dates
         WHERE rank_number = 1;
         ''')

    return pd.read_sql(statement, params={"start": start_date, "end": end_date}, con=engine)


def sort_data(result_set, sort_by, ascend):
    if type(sort_by) != list:
        raise TypeError("values need to be lists")

    if type(ascend) == list and len(sort_by) == len(ascend):
        result_set.sort_values(sort_by, ascending=ascend, inplace=True)

    ascend_list = ascend[:len(sort_by)] if type(ascend) == list else []

    for i in range(len(sort_by) - len(ascend_list)):
        ascend_list.append(True)

    result_set.sort_values(sort_by, ascending=ascend_list, inplace=True)


@app.get('/<start_date>/<end_date>')
def api_entrance(start_date, end_date):
    result_set = get_data_between_dates(start_date, end_date)

    result_set['description'].fillna("no description", inplace=True)
    result_set['description'].replace("", "no description", inplace=True)

    sort_params = request.json
    sort_by = sort_params.get("sort_by")
    ascend = sort_params.get("ascend")

    if sort_by:
        try:
            sort_data(result_set, sort_by, ascend)
        except TypeError as error:
            return error.__str__(), 400

    data = list_to_matrix(result_set.to_dict('records'))

    return data
