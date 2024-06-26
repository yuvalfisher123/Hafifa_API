import math

from sqlalchemy import text
from flask import Flask, request
import numpy as np
# from dbConnection import session

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')

app = Flask(__name__)


@app.get('/<start_date>/<end_date>')
def api_entrance(start_date, end_date):
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

    result_set = pd.read_sql(statement, params={"start": start_date, "end": end_date}, con=engine)

    result_set['description'].fillna("no description", inplace=True)
    result_set['description'].replace("", "no description", inplace=True)

    sort_params = request.json
    print(sort_params)
    sort_by = sort_params.get("sort_by")
    ascend = sort_params.get("ascend")

    if sort_by and ascend and len(sort_by) == len(ascend):
        if type(sort_by) != list or type(ascend) != list:
            return "values need to be lists", 400

        result_set.sort_values(sort_by, ascending=ascend, inplace=True)
    elif sort_by:
        ascend_list = ascend[:len(sort_by)] if ascend is not None else []

        for i in range(len(sort_by) - len(ascend_list)):
            ascend_list.append(True)

        result_set.sort_values(sort_by, ascending=ascend_list, inplace=True)

    data = result_set.to_dict('records')
    data_matrix = []

    for i in range(0, len(data), 10):
        data_matrix.append(data[i:i+10])

    return data_matrix
