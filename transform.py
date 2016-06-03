import numpy as np
import pandas as pd
import os
import math


def get_y(date, **option):
    '''get gaps by time slot according to specified date

    Notes:

    Argus: date: e.g. 2016-01-01

    Return:
        pandas DataFrame:
            columns: time_slot, district, gap
    '''


    # get hashToNumber and numberToHash

    hashToNumber, numberToHash = load_clusterMap('season_1/training_data/cluster_map/cluster_map')


    if option['folder'] == 'training':
        filedir = 'season_1/training_data/order_data/order_data_' + date
    elif option['folder'] == 'testing':
        filedir = 'season_1/test_set_1/order_data/order_data_' + date + '_test'
    else:
        print 'Please spcified folder name [training or testing]!!'

    orderNames = ['order_id', 'driver_id', 'passenger_id', 'start_district_hash', 'dest_district_hash', 'Price', 'Time']

    orderDf = pd.read_csv(filedir, sep='\t', header=None, names=orderNames, index_col='Time', parse_dates=True)

    # add time_slot column to orderDf
    timeSlotSeries = (orderDf.index.hour * 60 + orderDf.index.minute) / 10
    orderDf['time_slot'] = timeSlotSeries + 1

    # initialize the empty pandas dataframe
    # gapDf = pd.DataFrame()
    dfList = []
    for districtTuple in orderDf.groupby('start_district_hash'):
        # group by time_period column and count records (if driver_id == NaN, the record will not be counted )
        districtDf = districtTuple[1]
        requestAnswerDf = districtDf.groupby('time_slot').count()
        # requestAnswerDf[['driver_id', 'order_id']].plot()
        requestAnswerDf['gap'] = requestAnswerDf['order_id'] - requestAnswerDf['driver_id']
        requestAnswerDf['district_id'] = hashToNumber[districtTuple[0]]

        gapDf = requestAnswerDf[['district_id', 'order_id', 'driver_id', 'gap']]
        gapDf.rename(columns = {'order_id': 'request_num', 'driver_id': 'answer_num'},
                     index = lambda x: '{}-{}'.format(date, x), inplace=True)

        # print '{}, size:{}'.format(hashToNum[districtTuple[0]], gapDf.shape)
        dfList.append(gapDf)
        # yield gapMIxDf

    concatGapDf = pd.concat(dfList)

    return concatGapDf


def transform_weatherData(date, **option):
    '''
    '''
    if option['folder'] == 'testing':
        filedir = 'season_1/test_set_1/weather_data/weather_data_' + date + '_test'
    elif option['folder'] == 'training':
        filedir = 'season_1/training_data/weather_data/weather_data_' + date

    columnNames = ['Time', 'Weather', 'temperature', 'PM25']
    weatherDf = pd.read_csv(filedir, sep='\t', header=None, names=columnNames,\
                       index_col='Time', parse_dates=True)

    timeIndex = (weatherDf.index.hour * 60 + weatherDf.index.minute) / 10
    timeIndex += 1
    weatherDf['time_period'] = timeIndex

    date = weatherDf.index[0].strftime("%Y-%m-%d")
    weatherDropDf = weatherDf.drop_duplicates('time_period')
    # timeSlotList = ['{}-{}'.format(date, x) for x in weatherDropDf['time_period']]
    weatherDropDf.index = weatherDropDf['time_period']
    weatherDropDf.rename(index=lambda x: '{}-{}'.format(date, x), inplace=True)
    weatherDropDf.index.name = 'time_slot'

    return weatherDropDf

def transform_trafficData(date, **option):
    '''
    '''

    hashToNumber, numberToHash = load_clusterMap('season_1/training_data/cluster_map/cluster_map')

    if option['folder'] == 'testing':
        dirpath = 'season_1/test_set_1/traffic_data/traffic_data_' + date + '_test'
    elif option['folder'] == 'training':
        dirpath = 'season_1/training_data/traffic_data/traffic_data_' + date

    columnNames = ['districtHash', 'traffic_level1', 'traffic_level2', 'traffic_level3', 'traffic_level4', 'Time']
    trafficDf = pd.read_csv(dirpath, sep='\t', header=None, names=columnNames,\
                       index_col='Time', parse_dates=True)
    for col in columnNames:
        if col.startswith('traffic_level'):
            trafficDf[col] = trafficDf[col].map(lambda x: int(x.split(':')[1]))

    timeIndex = (trafficDf.index.hour * 60 + trafficDf.index.minute) / 10

    timeIndex += 1

    trafficDf['time_slot'] = timeIndex

    trafficDf['district_id'] = trafficDf['districtHash'].apply(lambda x: hashToNumber[x])

    trafficDf.index = trafficDf['time_slot']
    trafficDf.rename(index=lambda x: '{}-{}'.format(date, x), inplace=True)

    trafficFinalDf = trafficDf[['district_id', 'traffic_level1', 'traffic_level2', 'traffic_level3', 'traffic_level4']]

    #return trafficSlotDf
    return trafficFinalDf
