import numpy as np
import pandas as pd
from transform import get_y


def predict_by_window(yDisDf):
    '''within a district dataframe, predict by window function

    Notes: this function is specifically for a district's dataframe,
           used for a outside loop, groupby

    Argvs:
        yDisDf: a y dataframe get from get_y function, then groupby('district')

    Return:
        a


    '''
    testDfList = []
    colList = ['request_num', 'answer_num', 'gap']

    windowList = ['boxcar', 'triang', 'blackman', 'hamming', 'bartlett', 'parzen', 'bohman', 'blackmanharris', 'nuttall',
                  'barthann']

    # initialize a pandas dataframe with test indexes
    date = yDisDf['date'].iloc[0]
    districtId = yDisDf['district_id'].iloc[0]
    headColDf = pd.DataFrame(index = [i for i in range(46, 146, 12)])
    headColDf['date'] = date
    headColDf['district_id'] = districtId

    for winType in windowList:

        windowFunDf = yDisDf.rolling(window=3, win_type=winType).mean()
        rowList = []
        for index in range(46, 144, 12):
            # print 'processing slot {}'.format(index)
            # check whether the row exist
            if (index-1) in yDisDf.index:
                rowGet = windowFunDf.loc[index-1]
                rowGet.name = index
            elif (index-2) in yDisDf.index:
                rowGet = windowFunDf.loc[index-2]
                rowGet.name = index
            elif (index-3) in yDisDf.index:
                rowGet = windowFunDf.loc[index-3]
                rowGet.name = index
            else:
                print 'something wrong!!!'
                # assign a row record and make all values np.NaN
                rowGet = windowFunDf.iloc[0]
                for col in rowGet.keys():
                    rowGet['col'] = np.NaN

            rowList.append(rowGet)

        windowDf = pd.DataFrame(rowList)
        # windowDf.fillna(method='bfill', inplace=True)
        # rename columns names (add tag: window_type to request_num, answer_num and gap)
        # set up name dictionary
        colDict = {col:'{}_{}'.format(winType, col) for col in colList}
        windowDf.rename(columns=colDict, inplace=True)

        windowSubDf = windowDf[[col for col in colDict.values()]]
        # testDfList.append(windowSubDf)

        headColDf = headColDf.join(windowSubDf)

    # add a gap_mean column that take average of all gap values
    headColDf['win_mean'] = headColDf[['{}_gap'.format(winType) for winType in windowList]].mean(axis=1)

    return headColDf

def sum_predict_by_window():

    datesPredictDfList = []

    for i in range(22, 32, 2):
        date = '2016-01-{}'.format(i)
        print 'processing {} data...'.format(date)
        yDf = get_y(date, folder='testing')

        districtsPredictDfList = []

        for districtId, yDisDf in yDf.groupby('district_id'):
            print 'processing {} data...'.format(districtId)
            winPredictDf = predict_by_window(yDisDf)
            districtsPredictDfList.append(winPredictDf)

        districtsResultDf = pd.concat(districtsPredictDfList)
        datesPredictDfList.append(districtsResultDf)

    allPredictDf = pd.concat(datesPredictDfList)

    # allPredictDf[['date', 'district_id', 'win_mean']]

    allPredictDf.index.name = 'time_slot'

    finalDf = allPredictDf.set_index('district_id', append=True)[['date', 'win_mean']]

    # allPredictDf['date_slot'] = ["{}-{}".format(date, slot) for date, slot in\
    #                                 zip(allPredictDf['date'], allPredictDf.index)]

    # finalDf = allPredictDf[['date_slot', 'district_id', 'win_mean']]

    return finalDf



def predict_by_average(*dates):

    count = 0
    for date in dates:

        yDf = get_y(date, folder='training')
        distDfList = []
        renameColDict = {col:'{}_{}'.format(date, col) for col in ['request_num', 'answer_num', 'gap']}
        for district, yDisDf in yDf.groupby('district_id'):

            disDf = yDisDf.loc[range(1, 145)]\
                          .fillna(method='bfill')\
                          .fillna(method='ffill')\
                          .loc[range(46, 144, 12)][['district_id', 'request_num', 'answer_num', 'gap']]\
                          .rename(columns = renameColDict)

            distDfList.append(disDf)

        dateAveDf = pd.concat(distDfList)

        dateAveIndDf = dateAveDf.set_index('district_id', append=True)

        if count == 0:
            finalDateAveDf = dateAveIndDf
        else:
            finalDateAveDf = finalDateAveDf.join(dateAveIndDf, how='inner')
        count += 1


    finalOnlyDateAveDf = finalDateAveDf[['{}_gap'.format(date) for date in dates]]
    finalOnlyAveSeries = finalOnlyDateAveDf.mean(axis=1)
    finalOnlyAveDf = pd.DataFrame(finalOnlyAveSeries, columns=['ave_gap'])


    return finalOnlyAveDf


def sum_predict_by_average():

    dateRefDict = {22: ['15', '08'],\
                   24: ['17', '10'],\
                   26: ['05', '12', '19'],\
                   28: ['07', '14', '21'],\
                   30: ['09', '16']}


    datesPredictDfList = []

    for i in range(22, 32, 2):
        date = '2016-01-{}'.format(i)
        print 'processing {} data...'.format(date)

        dateRefList = dateRefDict[i]
        dates = ['2016-01-{}'.format(date) for date in dateRefList]

        dateDf = predict_by_average(*dates)

        datesPredictDfList.append(dateDf)

    finalDf = pd.concat(datesPredictDfList)
    return finalDf



