import os
import pandas as pd
import requests
import json

def get_BLS_data(seriesIDs, startyear, endyear):
    """
    get data from BLS
    """
    base_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'  #this will not change
    headers = {'Content-type': 'application/json'}  #This will not changed !

    # For the key seriesid enter a list of series names you wish to download
    # For the key startyear enter the start year inside ""
    # For the key endyear enter the end year inside ""
    
    parameters = { 
        "seriesid":seriesIDs,
        "startyear":str(startyear), 
        "endyear":str(endyear),
        "catalog":True, 
        "calculations":False, 
        "annualaverage":False,
        "aspects":False,
        "registrationkey":os.environ['BLS_API_key'] 
     }

    data = json.dumps(parameters) # Converts the Python dictionary to JSON

    p = requests.post(base_url, data=data, headers=headers)
    json_data = json.loads(p.text)
    
    message = ""
    if json_data['message']:
        #message = "For series " + seriesIDs + ", no data for years: "
        for i in range(len(json_data['message'])):
            message += json_data['message'][i][-4:] + ", "
    
    return message, json_data 
    
def create_year_increments(startyear, endyear):
    """
    Create start and endyear increments of 20 years or 
    """
    if endyear - startyear < 20:
        return startyear, endyear
    else:
        i = startyear
        increments = []
        while i <= endyear: 
            start_end_array = []
            start_end_array.append(i)
            if i + 19 > endyear:
                start_end_array.append(endyear)
            else:
                start_end_array.append(i + 19)
            increments.append(start_end_array)
            i += 20
    
    return increments
    
def format_data_dataframe(df_data):
    df_data['month'] = df_data['year'] + df_data['period'].str.replace('M','')
    df_data['month'] = pd.to_datetime(df_data['month'],format='%Y%m')
    df_data['month'] = df_data['month'].dt.date.apply(lambda x: x.strftime('%Y-%m'))
    df_data = df_data.sort_values(by=['month'])
    df_data['pct_changeMoM'] = df_data['value'].astype(float).pct_change() * 100
    df_data['pct_changeYoY'] = df_data['value'].astype(float).pct_change(12) * 100
    df_data['pct_changeMoM_Ann'] = ((df_data['value'].astype(float).pct_change() + 1) ** 12 - 1) * 100
    df_data = df_data.drop(columns=['year', 'period','periodName', 'latest','footnotes'])
    
    return df_data
    

def create_dict_dataframe(json_data):
    df_dict = pd.DataFrame()
    df_dict['series_title'] = json_data['Results']['series'][0]['catalog']['series_title'],
    df_dict['series_id'] = json_data['Results']['series'][0]['catalog']['series_id'],
    df_dict['seasonality'] = json_data['Results']['series'][0]['catalog']['seasonality'],
    df_dict['survey_long_name'] = json_data['Results']['series'][0]['catalog']['survey_name'],
    df_dict['survey_short_name'] = json_data['Results']['series'][0]['catalog']['survey_name'][-6:-1],
    df_dict['survey_abbreviation'] = json_data['Results']['series'][0]['catalog']['survey_abbreviation'],
    df_dict['measure_data_type'] = json_data['Results']['series'][0]['catalog']['measure_data_type'],
    df_dict['area'] = json_data['Results']['series'][0]['catalog']['area'],
    df_dict['item'] = json_data['Results']['series'][0]['catalog']['item']
    df_dict = df_dict.set_index(df_dict['series_id'])
    df_dict = df_dict.drop(columns=['series_id'])
    
    return df_dict
    
def create_dataframes(seriesID, startyear, endyear):
    increments = create_year_increments(startyear,endyear)
    i = 0
    df_data = pd.DataFrame() 
    df_dict = pd.DataFrame() 
    while i < len(increments): 
        message, json_data = get_BLS_data([seriesID], increments[i][0], increments[i][1])
        if not df_data.empty:
            df_data = pd.concat([df_data, pd.DataFrame(json_data['Results']['series'][0]['data'])])
        if df_data.empty:
            df_data = pd.DataFrame(json_data['Results']['series'][0]['data'])
        if df_dict.empty: 
            df_dict = create_dict_dataframe(json_data)
        i += 1
    
    df_data = format_data_dataframe(df_data)

    return df_data, df_dict  