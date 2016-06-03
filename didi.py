import numpy as np
import pandas as pd
import os
import math




def load_clusterMap(path):
    '''load district hash pair information

    Notes:

    Args:
        dir: the directory path to the cluster_map file

    Return:
        hashToNumber: a dictionary, hash(key) to number(value)
        numberToHash: a dictionary, number(key) to hash(value)

    '''
    hashToNumber = {}
    numberToHash = {}
    with open(path, 'r') as f:
        lines = f.readlines()
        # dataLines = data.split('\n')
        for line in lines:
            line = line.strip()
            # print line
            hashToNumber[line.split('\t')[0]] = line.split('\t')[1]
            numberToHash[line.split('\t')[1]] = line.split('\t')[0]

    return hashToNumber, numberToHash



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





def get_gap(date, **option):
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
        # gapDict = requestAnswerDf[['gap']].to_dict()
        # requestDict = requestAnswerDf[['order_id']].to_dict()
        # answerDict = requestAnswerDf[['driver_id']].to_dict()

        # return gapDict
        # suppliment info to no order time_slot
        # districtIdDict = {index: hashToNumber[districtTuple[0]] for index in xrange(1, 145)}
        # gapSupDict = {index: gapDict['gap'][index] if index in gapDict['gap'].keys() else 0 for index in xrange(1, 145)}


        # suppliment 0 to nonhappen time_slot
        # dfList = []
        # for index in range(1, 145):
        #    if index not in gapDf.index:
        #        gapDf.loc[index] = [hashToNum[districtTuple[0]], 0]

        # arrays = [np.array(districtIdDict.values()), np.array(districtIdDict.keys())]
        # tuples = list(zip(*arrays))

        # index = pd.MultiIndex.from_tuples(tuples, names=['districtNum', 'time_slot'])

        # gapMIxDf = pd.DataFrame(gapSupDict.values(), columns=['gap'], index=index)

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


def predict_by_average(*gapDfs):
    '''predict by average gaps of given dates (gapDf)

    Notes:

    Args: gap *dataframes

    Return:

    '''
    # time slots
    time_slots = range(46, 154, 12)

    for i, gapDf in enumerate(gapDfs):
        if i == 0:
            gapDfSum = gapDf
        else:
            gapDfSum += gapDf

    gapDfAverage = gapDfSum / len(gapDfs)

    # filter by time_slots
    gapPredictions = gapDfAverage.loc[np.array([True if item in range(46, 152, 12) else False for item in gapDf.index.get_level_values('time_slot')])]

    return gapPredictions




def calculate_mape(true, predict):
    '''calculate mape between true values and prediction values

    Notes:

    Args:
        true: a gapDf dataframe
        predict: a gapDf dataframe

    Return: mape value


    '''
    # nonZeroGap = ((true['gap'] != 0).value_counts()).loc[True]
    truePredictDf = ((true - predict) / true).applymap(lambda x: math.fabs(x)).replace(np.inf, np.nan)
    mape = truePredictDf.sum() / truePredictDf.count()

    return mape


def getCsv(gapDf, date):
    '''get the csv printout variable according to given gapDf

    Notes:


    Argvs:
        gapDf: the gap dataframe
        date: the date info used as a tag

    Return:
        csvTemp: a variable used for printing out
    '''
    indexList = gapDf.index.tolist()
    gapDf['district_id'] = [x[0] for x in indexList]
    gapDf['time_slot'] = ['{}-{}'.format(date, x[1]) for x in indexList]
    csvTemp = gapDf[['district_id', 'time_slot', 'gap']].to_csv(index=False, header=False)


    return csvTemp


def printCsv(label, predictDict):
    '''
    '''
    fileOut = 'predict_{}'.format(label)
    f = open(fileOut, 'w')
    for item in predictDict.keys():

        csvGet = getCsv(gapDf=predictDict[item], date=item)
        f.write(csvGet)
        # test()
    f.close()


