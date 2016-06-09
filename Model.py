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

    return headColDf




