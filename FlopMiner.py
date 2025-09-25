#!/usr/bin/env python
# coding: utf-8

# # Import Library and Load Data

# In[1]:


import pandas as pd
import numpy as np
#import libraries
get_ipython().system('pip3 install -U scikit-fuzzy')
import skfuzzy as fuzz
from skfuzzy import control as ctrl
import math
from typing import Dict
import timeit
get_ipython().system('pip install bitarray')
import copy


# In[2]:


data_aktivitas = pd.read_csv("D:\ADILA\College\TA\DATA\prepo\prepo_3h.csv")
data_aktivitas.tail()


# # Constructing Sequence

# In[3]:


#sequence dengan time interval di setiap aktivitas per hari
def extract_timestamp_new(activities, index_activities, time):
    temp = pd.DataFrame(activities)
    temp = temp.reset_index()
    temp = temp.iloc[:, 1:]
    start_time = 0
    end_time = 0
    interval_time = 0
    n = 0
    k = 0
    q = 0
    kegiatan_dict = []
    
    if len(activities) == 1:
        kegiatan_dict.append({temp[index_activities][0]:1})
        
    for i in range(0, len(temp)-1):
      if temp[index_activities][i] == temp[index_activities][i+1]:
        n += 1
        end_time = temp[time][i]
        if i == len(temp)-2:
          start_time = temp[time][i-n+1]
          interval_time = start_time, end_time
          kegiatan_dict.append({temp[index_activities][i]:interval_time})
      else:
        start_time = temp[time][i-n]
        end_time = temp[time][i]
        interval_time = start_time, end_time
        kegiatan_dict.append({temp[index_activities][i]:interval_time})
        if i == len(temp)-2:
          kegiatan_dict.append({temp[index_activities][i]:temp[time][i]})
        n = 0

    if len(kegiatan_dict) == 0:
        q += 1
        start_time = temp[time][i-q]
        end_time = temp[time][i]
        interval_time = start_time, end_time
        kegiatan_dict.append({temp[index_activities][i]:interval_time})
        
    if len(kegiatan_dict) > 1:
        if kegiatan_dict[0] == kegiatan_dict[1]:
            del kegiatan_dict[0]
    return kegiatan_dict


# In[4]:


#sequence dengan time interval di setiap aktivitas per hari
def split_activities_new(activities, index_activities):
    splitted = []
    counts = 0
    for i in range(0, activities.shape[0]-1):
        if activities[index_activities][i] == activities[index_activities][i+1]:
            if i == activities.shape[0]-2:
                splitted.append(activities.iloc[counts:i+2])
            continue
        else:
            splitted.append(activities.iloc[counts:i+1])
            counts = i+1
        if i == activities.shape[0]-2:
            splitted.append(activities.iloc[counts:i+2])
    return splitted


# In[5]:


#sequence dengan time interval per hari
splitted_activities_new = split_activities_new(data_aktivitas, 'Timestamp')
extracted_sequence_new = []
for i in range(len(splitted_activities_new)):
    splitted_activities_new[i] = splitted_activities_new[i].reset_index()
    extracted_sequence_new.append({splitted_activities_new[i].Timestamp[0] : extract_timestamp_new(splitted_activities_new[i], 'Activity', 'Time')})
    
extracted_sequence_new


# In[6]:


df = pd.DataFrame(extracted_sequence_new)
df_new = df.stack().reset_index()
df_new.columns = ['a','EID','Sequence']
del df_new['a']
df_new


# In[ ]:


# saving the sequence database
file_name = 'sequence_1d.xlsx'

df_new.to_excel(file_name)
print('DataFrame is written to Excel File successfully.')


# # Mining

# In[7]:


# extract identifier
def convertIdentifier(data): #ident
    getItemID = {}
    items = set()

    for day in data['Sequence']:
        for act in day:
            for i in act:
                get_item = sorted(list(set(act)))
                items.update(get_item)

    for item in items:
        for idx, row in data.iterrows():
            for day in row['Sequence']:
                for act in day:
                    if item in act:
                        ID = row['EID']
                        ID = int(ID[1:])
                        if item in getItemID:
                            getItemID[item].add(ID)
                        else:
                            getItemID[item] = set([ID])

    return getItemID, items

#extract timestamp
def get_timestamps1(start_time, end_time, item, data):
    
    ts_max = 5269391.729

    def get_st(input_data, item):
        for act in input_data:
            new_value = list(act.values())
            for i in act:
                if i == item:
                    for x in range(len(act)):
                        st_list = new_value[x]
                        st = (st_list[0])
                    break
        return st
    
    def get_et(input_data, item):
        for act in input_data:
            new_value = list(act.values())
            for i in act:
                if i == item:
                    for x in range(len(act)):
                        et_list = new_value[x]
                        et = (et_list[1])
                    break
        return et
                
        
    calc_st = get_st(data['Sequence'][start_time], item)
    calc_et = get_et(data['Sequence'][end_time], item)
    
    if calc_st == 0:
        st = 0
    else:
        st = calc_st
        
    if calc_et == 0:
        et = ts_max
    else:
        et = calc_et
                    
    return st, et

#calculate proportional duration
def calc_propdur(st, et):
    
    L = 5269391.729 #tsmax di database
    interval = et - st
    
    #fungsi gaussian
    def GaussMf(x,c):
        sigma = np.power(10, 5.5)
        if x > 0:
            return np.exp(-((x-c)**2)/(2*sigma**2)) 
        else:
            return 0
    
    #fuzzifikasi gaussian dengan 7 kategori
    D1 = GaussMf(interval, L / 8.)
    D2 = GaussMf(interval, L / 4.)
    D3 = GaussMf(interval, (L*3)/8)
    D4 = GaussMf(interval, L / 2.)
    D5 = GaussMf(interval, (L*5)/8)
    D6 = GaussMf(interval, (L*3)/4)
    D7 = GaussMf(interval, (L*7)/8)

    mf = np.where(D1 > D2, 'Very Short', 
            np.where(D2 > D3, 'Short',
            np.where(D3 > D4, 'Short to Normal',
            np.where(D4 > D5, 'Normal',
            np.where(D5 > D6, 'Normal to Long',
            np.where(D6 > D7, 'Long', 
                'Very Long'))))))
    
    md = np.where(D1 > D2, D1,
            np.where(D2 > D3, D2,
            np.where(D3 > D4, D3,
            np.where(D4 > D5, D4,
            np.where(D5 > D6, D5, 
            np.where(D6 > D7, D6, 
                D7))))))
    
    #defuzzifikasi
    if md == 0:
        propdur_value = 0
    else: 
        propdur_value = (md*interval)/L

    return propdur_value

#calculate time interval
def calc_intervals(temp, item, param, data):
    timeIntervals = []
    propdur = []
    maxPer = param[0]
    maxSoPer = param[1]
    minDur = param[2]
    tmax = 60
    left = -1
    count = 1
    preID = temp[0]
    ID = temp[1]
    
    while ID > 0:
        
        per = (ID - preID)/tmax
        
        # mencari titik awal interval
        if per <= maxPer and left == -1:
            left = preID
            soPer = maxSoPer

        # mencari titik akhir interval dan menentukan lfpp
        if left != -1:
            surPer = per - maxPer
            soPer = max(0, soPer + surPer)
            if soPer > maxSoPer:
                st, et = get_timestamps(left, preID, item, data)
                propdur_value = calc_propdur(st, et)
                if propdur_value >= minDur:
                    timeIntervals.append([st, et])
                    propdur.append(propdur_value)
                left = -1
            
        count += 1
        preID = ID
        ID = temp[count]

    # jika titik akhir adalah tmax
    per = (tmax - preID)/tmax
    if left != -1:
        surPer = per - maxPer
        soPer = max(0, soPer + surPer)
        if soPer > maxSoPer:
            st, et = get_timestamps(left, preID, item, data)
            propdur_value = calc_propdur(st, et)
            if propdur_value >= minDur:
                timeIntervals.append([st, et])
                propdur.append(propdur_value)
        else:
            st, et = get_timestamps(left, tmax, item, data)
            propdur_value = calc_propdur(st, et)
            if propdur_value >= minDur:
                timeIntervals.append([st, et])
                propdur.append(propdur_value)

    return timeIntervals, propdur


# # perluasan itemset

# In[ ]:


def get_result1(temp, item, param, data): #untuk panjang-1
    timeIntervals = []
    propdur = []
    maxPer = param[0]
    maxSoPer = param[1]
    minDur = param[2]
    tmax = 60
    count = 1
    left = -1
    preID = temp[0]
    ID = temp[1]
    
    while ID > 0:
        
        per = (ID - preID)/tmax
        surPer = per - maxPer
        
        # mencari titik awal interval
        if surPer <= maxPer and left == -1:
            left = preID
            soPer = maxSoPer

        # mencari titik akhir interval dan menentukan lfpp
        if left != -1:
            soPer = max(0, soPer + surPer)
            if soPer > maxSoPer:
                st, et = get_timestamps(left, preID, item, data)
                propdur_value = calc_propdur(st, et)
                if propdur_value >= minDur:
                    timeIntervals.append([st, et])
                    propdur.append(propdur_value)
                left = -1
                
        count += 1
        preID = ID
        ID = temp[count]

    # jika titik akhir adalah tmax
    per = (tmax - preID)/tmax
    if left != -1:
        surPer = per - maxPer
        soPer = max(0, soPer + surPer)
        if soPer > maxSoPer:
            st, et = get_timestamps(left, preID, item, data)
            propdur_value = calc_propdur(st, et)
            if propdur_value >= minDur:
                timeIntervals.append([st, et])
                propdur.append(propdur_value)
        else:
            st, et = get_timestamps(left, tmax, item, data)
            propdur_value = calc_propdur(st, et)
            if propdur_value >= minDur:
                timeIntervals.append([st, et])
                propdur.append(propdur_value)

    return timeIntervals, propdur

def get_result(term, panjang_term, items, getItemID, param, data): #untuk panjang >= 2
    lfpp = []
    timeIntervals = []
    propdur = []

    if len(items) <= 1: #panjang=1
        return lfpp, timeIntervals, propdur

    termId = copy.deepcopy(getItemID[term[0]])
    for i in range(1, panjang_term):
        termId &= (getItemID[term[i]])

    if len(items) == 2: #panjang=2
        itemI = items[0]
        idSetI = getItemID[0]
        termId &= (getItemID[itemI])

        itemJ = items[1]
        idSetJ = getItemID[1]
        termId &= (getItemID[itemJ])

        ts, pd = calc_intervals(list(termId), items, param, data)
        if len(ts) > 0:
            new_panjang_term = panjang_term + 1
            term[panjang_term] = itemI
            
            lfpp.append(termId)
            timeIntervals.append(ts)
            propdur.append(pd)
            
        return lfpp, timeIntervals, propdur


    for i in range(len(items) - 1): #panjang>2
        itemI = items[i]
        idSetI = copy.deepcopy(termId)
        idSetI &= (getItemID[itemI])

        SuffixItems = []

        new_panjang_term = panjang_term + 1
        term[panjang_term] = itemI

        for j in range(i + 1, len(items)):
            itemJ = items[j]

            idSetIJ = copy.deepcopy(idSetI)
            idSetIJ &= (getItemID[itemJ])

            ts, pd = calc_intervals(list(idSetIJ), items, param, data)

            if len(ts) > 0:
                SuffixItems.append(itemJ)

                lfpp.append(idSetIJ)
                timeIntervals.append(ts)
                propdur.append(pd)


        if len(SuffixItems) > 0:
            gp, time, dur = get_result(term, new_panjang_term, SuffixItems, getItemID, param, data)
            
            if gp not in lfpp and time not in timeIntervals and dur not in propdur:
                lfpp.extend(gp)
                timeIntervals.extend(time)
                propdur.extend(dur)
        
    return lfpp, timeIntervals, propdur


# # Test

# In[ ]:


def FLoPMiner(data, maxPer, maxSoPer, minDur, file_name):
    
    param = [maxPer, maxSoPer, minDur]

    getItemID, getItemID_items = convertIdentifier(data)
    
    pattern = []
    lfpp = []
    ts_list = []
    propdur_list = []
    
    for item in getItemID_items: #result item panjang-1
        ts, pd = get_result1(list(getItemID[item]), item, param, data)
        if ts > 0:
            pattern.append(item)
            lfpp.append(item)
            ts_list.append(ts)
            propdur_list.append(pd)
        
    for i in range(len(pattern) - 1): #result item panjang>=2
        itemI = pattern[i]
        idSetI = getItemID[itemI]            
        items = []

        for j in range(i + 1, len(pattern)):
            itemJ = pattern[j]
            idSetJ = getItemID[itemJ]
                
            idSetIJ = idSetI.copy()
            idSetIJ &= idSetJ
                
            timeIntervals, propDur = calc_intervals(list(idSetIJ), itemJ, param, data)
                
            if timeIntervals > 0:
                items.append(idSetIJ)
                lfpp.append(idSetIJ)
                ts_list.append(timeIntervals)
                propdur_list.append(propDur)
                
        #ekspansi itemset
        if len(items) > 0:
            gp, ts, pd = get_result(list(range(1, len(items)+1)), items, idSetI, getItemID, param, data)
            
            lfpp.append(gp)
            ts_list.append(ts)
            propdur_list.append(pd)
    
    LFPP_data = {'Pattern': lfpp,
                'Periodic Fuzzy Time Interval': ts_list,
                'Proportional Duration': propdur_list} 
    
    LFPP_df = pd.DataFrame(LFPP_data)
    
    if 0 < len(LFPP_df) < 1048575:
        #save the output
        LFPP_df.to_excel(file_name)
        print('DataFrame is written to Excel File successfully.')
    
    return LFPP_df


# In[ ]:


# initiate parameters
maxPer = 0.1
maxSoPer = 0.05
minDur = 0.7

start = timeit.default_timer()

#run algorithm
FLoPMiner(df_new, maxPer, maxSoPer, minDur, 'output_3h.xlsx')

stop = timeit.default_timer()
execution_time = stop - start
print("Program Executed in "+str(execution_time)+" seconds")
