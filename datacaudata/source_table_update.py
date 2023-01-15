from datetime import datetime
import json
import time

import pandas as pd
import requests
from sqlalchemy.exc import ProgrammingError

from db_connection import ENGINE


OREGON_PLACE_ID = 10
WASHINGTON_PLACE_ID = 46
CAUDATA_TAXON_ID = 26718

COLUMNS = [
    'inaturalist_id', 'observed_date', 'observed_month', 'observed_hour', 'observed_week', 
    'observed_year', 'observed_day', 'species_guess', 'identifications_most_disagree', 'place_ids', 
    'location', 'endemic', 'native', 'introduced', 'threatened', 
    'name', 'rank', 'taxon_id', 'wikipedia_url', 'preferred_common_name'
]

DB_TABLE = 'washington_oregon_salamanders'


def get_observations(params):
    response = requests.get(
        'https://api.inaturalist.org/v1/observations', 
        params=params
        )
    return response

def create_observations_dataframe(place_id, date_after=None):
    start_time = datetime.now()
    salamander_dict = {k: [] for k in COLUMNS}
    params = {
    'taxon_id': CAUDATA_TAXON_ID,
    'place_id': place_id,
    'captive': 'false',
    'per_page': 100
    }

    if date_after is not None:
        params['d1'] = date_after
    
    request = get_observations(params=params)
    request_json = request.json()

    total_results = request_json['total_results']
    results_per_page = request_json['per_page']
    num_pages = int(total_results / results_per_page) + 1
    
    counter = 1
    params['page'] = 0 # add a starting page to the params dict
    for n in range(num_pages):
        params['page'] += 1
        request = get_observations(params=params)
        request_json = request.json()['results']
        
        for r in request_json:
            salamander_dict['inaturalist_id'].append(r.get('id'))
            observed_on_details = r.get('observed_on_details', {})
            # there are times when the 'observed_on_details' key is present, but with a None value

            if observed_on_details is None: 
                observed_on_details = {}
            salamander_dict['observed_date'].append(observed_on_details.get('date'))
            salamander_dict['observed_month'].append(observed_on_details.get('month'))
            salamander_dict['observed_hour'].append(observed_on_details.get('hour'))
            salamander_dict['observed_week'].append(observed_on_details.get('week'))
            salamander_dict['observed_year'].append(observed_on_details.get('year'))
            salamander_dict['observed_day'].append(observed_on_details.get('day'))
            salamander_dict['species_guess'].append(r.get('species_guess'))
            salamander_dict['identifications_most_disagree'].append(r.get('identifications_most_disagree'))
            salamander_dict['place_ids'].append(r.get('place_ids'))
            salamander_dict['location'].append(r.get('location'))
            taxon = r.get('taxon', {})
            salamander_dict['endemic'].append(taxon.get('endemic'))
            salamander_dict['native'].append(taxon.get('native'))
            salamander_dict['introduced'].append(taxon.get('introduced'))
            salamander_dict['threatened'].append(taxon.get('threatened'))
            salamander_dict['name'].append(taxon.get('name'))
            salamander_dict['rank'].append(taxon.get('rank'))
            salamander_dict['taxon_id'].append(taxon.get('id'))
            salamander_dict['wikipedia_url'].append(taxon.get('wikipedia_url'))
            salamander_dict['preferred_common_name'].append(taxon.get('preferred_common_name'))
    
        counter += 1
        time_passed = (datetime.now() - start_time).seconds

        # crudely avoiding ratelimiting
        if time_passed % 30 >= 29 and counter >= 29:
            counter = 0
            time.sleep(60)
        
    df = pd.DataFrame(salamander_dict)
    df['observed_date'] = pd.to_datetime(df['observed_date'])

    return df

def write_observations_dataframe_to_db(dataframe):
    dataframe.to_sql(
        DB_TABLE,
        ENGINE,
        if_exists='append',
        index=False
        )

def remove_duplicate_observations(dataframe):
    # dedupe based on inaturalist_id
    existing_inaturalist_id = f'''
        SELECT DISTINCT 
             inaturalist_id
        FROM {DB_TABLE}
    '''
    df_existing_inaturalist_id = pd.read_sql(
        existing_inaturalist_id,
        con=ENGINE,
        index_col=None
    )
    return dataframe.loc[~dataframe['inaturalist_id'].isin(df_existing_inaturalist_id['inaturalist_id'])]

def main():
    max_observed_date = f'''
        SELECT 
             MAX(observed_date) as max_observed_date
        FROM {DB_TABLE}
    '''

    try:
        df = pd.read_sql(
                max_observed_date,
                con=ENGINE,
                index_col=None
            )
        date_after = df['max_observed_date'][0].strftime('%Y-%m-%d')
        print(f'getting observations from {date_after} onward')
    except ProgrammingError:
        # the table has not been created, so we're starting from "scratch"
        date_after = None
    
    for place in [WASHINGTON_PLACE_ID, OREGON_PLACE_ID]:
        df = create_observations_dataframe(place, date_after=date_after)
        df_deduped = remove_duplicate_observations(df)
        write_observations_dataframe_to_db(df_deduped)
        print(f'{len(df_deduped.index)} new rows added to {DB_TABLE}')


if __name__ == '__main__':
    main()