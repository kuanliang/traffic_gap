import numpy as np
import pandas as pd
import os
import math



class dataLoader():

    def load_clusterMap(self, path):
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

class explorer():
    '''the explorer will do some basic explore of the data
    '''
    def __init__(self, numberToHash):
        self.numberToHash = numberToHash

    def plot_request_answer(self, *dateList, **district):
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



class transformer():


    def __init__(self, hashToNumber, numberToHash):
        self.hashToNumber = hashToNumber
        self.numbertoHash = numberToHash

    def get_gap(self, date, **option):
        '''get gaps by time slot according to specified date

        Notes:

        Argus: date: e.g. 2016-01-01

        Return:
            pandas DataFrame:
                columns: time_slot, district, gap
        '''
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

            gapDict = requestAnswerDf[['gap']].to_dict()

            # return gapDict
            # suppliment infor to no order time_slot
            districtIdDict = {index: self.hashToNumber[districtTuple[0]] for index in range(1, 145)}
            gapSupDict = {index: gapDict['gap'][index] if index in gapDict['gap'].keys() else 0 for index in xrange(1, 145)}


            # suppliment 0 to nonhappen time_slot
            # dfList = []
            # for index in range(1, 145):
            #    if index not in gapDf.index:
            #        gapDf.loc[index] = [hashToNum[districtTuple[0]], 0]

            arrays = [np.array(districtIdDict.values()), np.array(districtIdDict.keys())]
            tuples = list(zip(*arrays))

            index = pd.MultiIndex.from_tuples(tuples, names=['district_id', 'time_slot'])

            gapMIxDf = pd.DataFrame(gapSupDict.values(), columns=['gap'], index=index)


            # print '{}, size:{}'.format(hashToNum[districtTuple[0]], gapDf.shape)
            dfList.append(gapMIxDf)
            # yield gapMIxDf

        concatGapDf = pd.concat(dfList)

        return concatGapDf


class predictor():
    '''predict gap values by district and time_slot

    '''
    def predict_by_average(self, *gapDfs):
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



class evaluator():


    def calculate_mape(self, true, predict):
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








