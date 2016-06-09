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
            # check whether the row exist
            if (index-1) in yDisDf.index:
                rowGet = windowFunDf.loc[index-1]
                rowGet.name = index
            elif (index-2) in yDisDf.index:
                rowGet = windowFunDf.loc[index-2]
                rowGet.name = index
            elif (index-3) in yDisDf.index:
                rowGet = windowFunDf.loc[index-2]
                rowGet.name = index
            else:
                print 'something wrong!!!'

            rowList.append(rowGet)

        windowDf = pd.DataFrame(rowList)
        # rename columns names (add tag: window_type to request_num, answer_num and gap)
        # set up name dictionary
        colDict = {col:'{}_{}'.format(winType, col) for col in colList}
        windowDf.rename(columns=colDict, inplace=True)

        windowSubDf = windowDf[[col for col in colDict.values()]]
        # testDfList.append(windowSubDf)

        headColDf = headColDf.join(windowSubDf)

    # add a gap_mean column that take average of all gap values
    headColDf['gap_mean'] = headColDf[['{}_gap'.format(winType) for winType in windowList]].mean(axis=1)

    return headColDf

def sum_predict_by_window():

    datePredictDfList = []

    for date in range(22, 32, 2):
        yDf = get_y(date, folder='testing')

        districtsPredictDfList = []

        for districtId, yDisDf in yDf.groupby('district_id')
            winPredictDf = predict_by_window(yDisDf)
            districtPredictDfList.append(winPredictDf)

        districtsResultDf = pd.concat(districtPredictDfList)
        datesPredictDfList.append(districtsResultDf)

    allPredcitDf = pd.concat(datesPredictDfList)
    return allPredcitDf








