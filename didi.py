def get_request_answer(*dateList, **district):
    '''get request, answer, gap numbers of specified date

    Notes:
        if the dictionary district is not specified,

        else,


    Args:
        dateList(list):

        district(dictionary):

    Return:

    '''
    # read in cluster_map data, and store hash id mapping info to a dictionary clusterMap
    hashToNumber = {}
    numberToHash = {}
    with open('season_1/training_data/cluster_map/cluster_map', 'r') as f:
        lines = f.readlines()
        # dataLines = data.split('\n')
        for line in lines:
            line = line.strip()
            # print line
            hashToNumber[line.split('\t')[0]] = line.split('\t')[1]
            numberToHash[line.split('\t')[1]] = line.split('\t')[0]

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
            orderHashDf = orderDf[orderDf['start_district_hash'] == numberToHash[district['districtNum']]]
            timeIndex = (orderHashDf.index.hour * 60 + orderHashDf.index.minute) / 10
            timeIndex += 1
            orderHashDf['time_period'] = timeIndex

            requestAnswerDf = orderHashDf.groupby('time_period').count()

            requestAnswerDf['gap'] = requestAnswerDf['order_id'] - requestAnswerDf['driver_id']

            requestAnswerDf[['order_id','driver_id']].plot()




def get_districtHash_number():
    '''
    '''
    # read in cluster_map data, and store hash id mapping info to a dictionary clusterMap
    hashToNumber = {}
    numberToHash = {}
    with open('season_1/training_data/cluster_map/cluster_map', 'r') as f:
        lines = f.readlines()
        # dataLines = data.split('\n')
        for line in lines:
            line = line.strip()
            # print line
            hashToNumber[line.split('\t')[0]] = line.split('\t')[1]
            numberToHash[line.split('\t')[1]] = line.split('\t')[0]

    return hashToNumber, numberToHash

def get_gap(date):
    '''get gaps by time slot according to specified date

    Notes:

    Argus: date: e.g. 2016-01-01

    Return:
        pandas DataFrame:
            columns: time_slot, district, gap
    '''

    filedir = 'season_1/training_data/order_data/order_data_' + date
    orderNames = ['order_id', 'driver_id', 'passenger_id', 'start_district_hash', 'dest_district_hash', 'Price', 'Time']

    orderDf = pd.read_csv(filedir, sep='\t', header=None, names=orderNames, index_col='Time', parse_dates=True)

    # add time_slot column to orderDf
    timeSlotSeries = (orderDf.index.hour * 60 + orderDf.index.minute) / 10
    orderDf['time_slot'] = timeSlotSeries + 1

    # get hash id mapping information
    hashToNum, numToHash = get_districtHash_number()

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
        districtIdDict = {index: hashToNumber[districtTuple[0]] for index in range(1, 145)}
        gapSupDict = {index: gapDict['gap'][index] if index in gapDict['gap'].keys() else 0 for index in range(1, 145)}


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

    concatGapDf = pd.concat(dfList)

    return concatGapDf

def predict_by_average(*training_dates):
    '''predict the gap of each time slot according to average gaps from specified dates

    Notes:
        different districts are predicted spearately

    Argus:
        target_date: the date being predicted, e.g. 2016-01-01
        training_date: a tuple including date used for training the model

    Return:
        a DataFrame record order_number, answer_number with time_period indexed

    '''
    # time slots
    time_slots = range(46, 154, 12)

    # column names
    orderNames = ['order_id', 'driver_id', 'passenger_id', 'start_district_hash', 'dest_district_hash', 'Price', 'Time']

    # initialize the gapDf
    gapDf = get_gap(training_dates[0])
    gapDf['gap'] = 0
    for date in training_dates:
        gapDf += get_gap(date)

    gapDf = gapDf / len(training_dates)

    # filter by time_slots
    gapSubDf = gapDf.loc[np.array([True if item in range(46, 152, 12) else False for item in gapDf.index.get_level_values('time_slot')])]

    return gapSubDf



def calculate_mape(true, predict):
    '''calculate MAPE

    Notes:

    Args:
        true: a dataframe
        predict: a dataframe

    Return: mape


    '''
    nonZeroGap = ((true['gap'] != 0).value_counts()).loc[True]
    truePredictDf = ((true - predict) / true).applymap(lambda x: math.fabs(x)).replace(np.inf, np.nan)
    mape = truePredictDf.sum() / nonZeroGap

    return mape
