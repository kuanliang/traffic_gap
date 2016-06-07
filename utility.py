import numpy as np
import pandas as pd

def fillTestRecord(df):

    if len(df.columns) == 5:
        # initialize the record and set all columns to np.NaN
        rowTemp = df.iloc[0]
        # set all values to NaN
        for key in rowTemp.keys():
            rowTemp[key] = np.NaN

        for i in range(46, 146, 12):
            df.loc[i] = rowTemp

        df.sort_index(inplace=True)
        df.fillna(method='ffill', inplace=True)

        return df

    elif len(df.columns == 7):
    # initialize the temp record and set all values to np.NaN
        rowTemp = df.iloc[0]
        for key in rowTemp.keys():
            rowTemp[key] = np.NaN

        dfList = []
        for districtId, subDf in df.groupby('district_id'):
            # print districtId

            # insert test record with record filled with NaN
            for i in range(46, 146, 12):
                subDf.loc[i] = rowTemp

            subDf.sort_index(inplace=True)
            subDf.fillna(method='ffill', inplace=True)
            subDf['slot'] = subDf.index

            subDf['district_id'] = districtId
            dfList.append(subDf)

        finalDf = pd.concat(dfList)

        return finalDf
