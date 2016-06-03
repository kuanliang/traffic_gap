import numpy as np
import pandas as pd

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
