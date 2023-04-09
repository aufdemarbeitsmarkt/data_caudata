from datetime import datetime
import re

import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA

from db_connection import ENGINE


DB_TABLE = 'forecasts'

MONTHLY_SUMMARY = '''
    SELECT *
    FROM public.forecast_prep__monthly_species_summary
'''.strip()

MONTHLY_SUMMARY_BY_LOCATION = '''
    SELECT *
    FROM public.forecast_prep__monthly_species_summary_by_location
'''.strip()

QUERIES_TO_RUN = [MONTHLY_SUMMARY, MONTHLY_SUMMARY_BY_LOCATION]


def read_sql_query(query_string):
    df = pd.read_sql(
        query_string,
        con=ENGINE,
        parse_dates=True
    )
    return df

def create_forecast(df):
    sf = StatsForecast(
        models = [AutoARIMA(season_length = 12)],
        freq = 'M'
    )

    sf.fit(df[:-1])

    forecast_df = sf.predict(h=4, level=[90])
    return forecast_df

def prepare_forecast_df(df, forecast_type):
    df = df.reset_index()

    df = df.rename(
        columns={
            ''
            'AutoARIMA': 'auto_arima',
            'AutoARIMA-lo-90': 'auto_arima_lo_90',
            'AutoARIMA-hi-90': 'auto_arima_hi_90'
            }
        )
    df['_date_forecasted'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
    df['forecast_type'] = forecast_type
    
    return df

def write_forecast_to_db(df):
    df.to_sql(
        DB_TABLE,
        ENGINE,
        if_exists='append',
        index=False
        )

def main():
    for query in QUERIES_TO_RUN:
        df = read_sql_query(query)
        # note: table_forecasted will have to be adjusted if I ever put, for example, a WHERE clause in any of the above queries
        table_forecasted = re.split('[.\n](forecast_prep__)', query)[-1]
        forecast_df = prepare_forecast_df(
            create_forecast(df), forecast_type=table_forecasted 
            )
        
        write_forecast_to_db(forecast_df)
        print(f'{len(forecast_df.index)} new rows added to `{DB_TABLE}`, generated from `{table_forecasted}`\n-----')
    

if __name__ == '__main__':
    main()
