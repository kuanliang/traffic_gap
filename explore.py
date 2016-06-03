import numpy as np
import pandas as pd
import os
import math

def plot_request_answer(*dateList, **district):
    '''get request, answer, gap numbers of specified date

    Notes:
        if the dictionary district is not specified,

        else,


    Args:
        dateList(list):

        district(dictionary):

    Return: None

    '''

    # column names
    orderNames = ['order_id', 'driver_id', 'passenger_id', 'start_district_hash', 'dest_district_hash', 'Price', 'Time']

    if not district:

        filedir = 'season_1/training_data/order_data/order_data_' + dateList[0]

        orderDf = pd.read_csv(filedir, sep='\t', header=None, names=orderNames, index_col='Time', parse_dates=True)

        for startDistrict in orderDf.groupby('start_district_hash'):
            # print startDistrict[0]
            startDistrictDf = startDistrict[1]
            # add a column to specify time_period index
            timeIndex = (startDistrictDf.index.hour * 60 + startDistrictDf.index.minute) / 10
            timeIndex += 1
            startDistrictDf['time_period'] = timeIndex

            # group by time_period column and count records (if driver_id == NaN, the record will not be counted )
            requestAnswerDf = startDistrictDf.groupby('time_period').count()

            # requestAnswerDf[['driver_id', 'order_id']].plot()
            requestAnswerDf['gap'] = requestAnswerDf['order_id'] - requestAnswerDf['driver_id']

            requestAnswerDf[['order_id','driver_id']].plot()

    else:
        for date in dateList:
            filedir = 'season_1/training_data/order_data/order_data_' + date
            orderDf = pd.read_csv(filedir, sep='\t', header=None, names=orderNames, index_col='Time', parse_dates=True)

            # specify the start_district
            orderHashDf = orderDf[orderDf['start_district_hash'] == self.numberToHash[district['districtNum']]]
            timeIndex = (orderHashDf.index.hour * 60 + orderHashDf.index.minute) / 10
            timeIndex += 1
            orderHashDf['time_period'] = timeIndex

            requestAnswerDf = orderHashDf.groupby('time_period').count()

            requestAnswerDf['gap'] = requestAnswerDf['order_id'] - requestAnswerDf['driver_id']

            requestAnswerDf[['order_id','driver_id']].plot()
