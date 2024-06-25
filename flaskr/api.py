from sqlalchemy import text
from flask import Flask
from dbConnection import session

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
     WHERE rank_number = 1;''')

    result = session.execute(statement, {'start': start_date, 'end': end_date}).mappings()
    result = [{**row} for row in result]

    return result
