import numpy as np
import pandas as pd
import os
import math



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


