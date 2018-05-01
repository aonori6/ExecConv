#_*_ coding: utf-8 _*_

import time
import csv
import os
import pandas as pd
import glob
import re
import datetime
from functools import cmp_to_key
import gc

def cmp(a, b):
    if a == b: return 0
    return -1 if a < b else 1

def cmptuple(a, b):
    return cmp(int(a[0]), int(b[0]))

if __name__ == '__main__':

    prefix = "./executions_"
    suffix = ".csv"
    csv_list = glob.glob(prefix+"*"+suffix)
    csv_list_tupled = [(re.search("\d+", x).group(), x) for x in csv_list]
    csv_list_tupled.sort(key=cmp_to_key(cmptuple))
    csv_list = [x[1] for x in csv_list_tupled]
    csv_list.reverse()

    count = 0
    df_list = []
    for csv_path in csv_list:
        count += 1
        print(count, csv_path)
        df = pd.read_csv(csv_path)

        # reduce memory
        df['id'] = df['id'].astype('int32')
        df['price'] = df['price'].astype('float32')
        df['size'] = df['size'].astype('float32')
        df = df.drop('side', axis=1)
        df = df.drop('buy_child_order_acceptance_id', axis=1)
        df = df.drop('sell_child_order_acceptance_id', axis=1)

        df = df.sort_values(by=['id'], ascending=True)
        df = df.drop('id', axis=1)
        df_list.append(df)

    # brute force
    # df = pd.concat(df_list)

    # reduce memory
    df = pd.DataFrame(index=[], columns=[])
    for dummy in range(len(df_list)):
        df = pd.concat([df,df_list[0]])
        del df_list[0]
        gc.collect()

    dt_exec = []
    exec_date = df['exec_date'].values.tolist()
    # reduce memory
    df = df.drop('exec_date', axis=1)
    for date in exec_date:
        if re.search(":\d\d$", date):
            date = date + ".0"
        dt = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')
        dt = dt + datetime.timedelta(hours=9) # UTC -> JPY
        dt_exec.append(dt)

    # reduce memory
    price = df['price'].values.tolist()
    df = df.drop('price', axis=1)
    size = df['size'].values.tolist()
    df = df.drop('size', axis=1)
    del df
    gc.collect()

    if not ( len(dt_exec) == len(price) and len(price) == len(size) ):
        print("---warn---")
    else:
        print('List size:',len(dt_exec))

    # convert exec to ohlc
    time_old = dt_exec[0].replace(second=0,microsecond=0)
    openprice = price[0]
    high = price[0]
    low = price[0]
    amount = 0
    ohlc = []
    for i in range(len(dt_exec)):
        if ( time_old == dt_exec[i].replace(second=0,microsecond=0) ):
            high = max(high, price[i])
            low = min(low, price[i])
            amount = amount + size[i]
        else:
            ohlc.append([time_old, openprice, high, low, price[i-1], amount])
            print(time_old, openprice, high, low, price[i-1], amount)
            
            time_old = dt_exec[i].replace(second=0,microsecond=0)
            openprice = price[i]
            high = price[i]
            low = price[i]
            amount = size[i]
    ohlc.append([time_old, openprice, high, low, price[i], amount])

    with open('ohlc.csv', 'w') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerows(ohlc)
