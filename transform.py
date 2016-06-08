import numpy as np
import pandas as pd
import os
import math
from load import load_clusterMap

from utility import fillTestRecord




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
        requestAnswerDf['slot'] = requestAnswerDf.index
        requestAnswerDf['date'] = date

        gapDf = requestAnswerDf[['date', 'slot', 'district_id', 'order_id', 'driver_id', 'gap']]
        gapDf.rename(columns = {'order_id': 'request_num', 'driver_id': 'answer_num'}, inplace=True)

        if option['folder'] == 'training':
            # add rolling window column
            newColDf = gapDf[['request_num', 'answer_num', 'gap']]\
                            .rolling(window=3, win_type='triang', center=True).mean()

            colNameDict = {ori:'{}_rolling'.format(ori) for ori in newColDf.columns}

            # rename column names with _rolling
            newColDf.rename(columns=colNameDict, inplace=True)

            # concate new columns (vertically concat two dataframe)
            gapRollDf = pd.concat([gapDf, newColDf], axis=1)

        elif option['folder'] == 'testing':
            # specified test dates
            testDatesList = range(46, 144, 12)
            # add testing record and set it the same as previous record
            for testDate in testDatesList:
                record = gapDf[gapDf['slot'] == (testDate-1)]
                # print record
                # print '\n'
                record['slot'] = testDate
                gapDf.append(record)
                # print gapDf
                # print '\n'
                gapRollDf = gapDf

        # print gapRollDf

        # print '{}, size:{}'.format(hashToNum[districtTuple[0]], gapDf.shape)
        dfList.append(gapRollDf)
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
    weatherDf['slot'] = timeIndex


    date = weatherDf.index[0].strftime("%Y-%m-%d")
    weatherDropDf = weatherDf.drop_duplicates('slot')
    weatherDropDf['date'] = date
    # timeSlotList = ['{}-{}'.format(date, x) for x in weatherDropDf['slot']]
    weatherDropDf.index = weatherDropDf['slot']
    # weatherDropDf.rename(index=lambda x: '{}-{}'.format(date, x), inplace=True)
    weatherDropDf.index.name = 'time_slot'
    # del weatherDropDf['time_period']



    weatherFinalDf = weatherDropDf[['date', 'slot', 'Weather', 'temperature', 'PM25']]


    if option['folder'] == 'testing':
        weatherFinalDf = fillTestRecord(weatherFinalDf)


    return weatherFinalDf

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

    trafficDf['slot'] = timeIndex

    trafficDf['district_id'] = trafficDf['districtHash'].apply(lambda x: hashToNumber[x])

    trafficDf.index = trafficDf['slot']
    # trafficDf.rename(index=lambda x: '{}-{}'.format(date, x), inplace=True)

    trafficDf['date'] = date

    trafficFinalDf = trafficDf[['date', 'slot', 'district_id', 'traffic_level1', 'traffic_level2', 'traffic_level3', 'traffic_level4']]

    if option['folder'] == 'testing':
        trafficFinalDf = fillTestRecord(trafficFinalDf)

    #return trafficSlotDf
    return trafficFinalDf

def filterTimeSlot(gapDf):
    gapFilterDf = gapDf.loc[np.array([True if item in range(46, 152, 12) else False for item in\
                                             gapDf.index.get_level_values('time_slot')])]
    return gapFilterDf


def create_matrix():
    '''

    '''
    trainDayList = ["%.2d" % i for i in range(1, 22)]
    testDayList = range(22,32,2)
    # dayList = ['01']
    dfList = []
    for day in trainDayList:
        date = '2016-01-' + day
        print 'transforming {} training data...'.format(date)
        trafficSlotDf = transform_trafficData(date, folder='training')

        # print trafficDf.index[0]
        weatherSlotDf= transform_weatherData(date, folder='training')

        ySlotDf = get_y(date, folder='training')

        tempDf = trafficSlotDf.merge(weatherSlotDf, how='left', on='slot')\
                              .sort_values(by=['district_id', 'slot'])
        tempFillDf = tempDf.fillna(method='ffill')
        mergedDf = tempFillDf.merge(ySlotDf, how='left', on=['district_id', 'slot'])

        dfList.append(mergedDf)

    finalTrainDf = pd.concat(dfList)

    dfList = []
    for day in testDayList:
        date = '2016-01-' + str(day)
        print 'trasforming {} testing data...'.format(date)
        trafficSlotDf = transform_trafficData(date, folder='testing')

        # print trafficDf.index[0]
        weatherSlotDf= transform_weatherData(date, folder='testing')

        ySlotDf = get_y(date, folder='testing')

        tempDf = trafficSlotDf.merge(weatherSlotDf, how='left', on='slot')\
                              .sort_values(by=['district_id', 'slot'])
        tempFillDf = tempDf.fillna(method='ffill')

        mergedDf = tempFillDf.merge(ySlotDf, how='left', on=['district_id', 'slot'])

        dfList.append(mergedDf)

    finalTestDf = pd.concat(dfList)

    return finalTrainDf, finalTestDf






