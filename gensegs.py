# gensegs.py
# last revised 21 Jun 2016

# This is a script to generate data that mimics database records associating
# user IDS with segment categories. 

# usage: python gensegs.py > master_files/1Msegs.csv

import random
import sys
import json

'''
user_id: int  # for simplicity we use integer index
age_id: string
user_seg: string 
'''
 
N_test = 1000000  # number of simulated data items
N_group1 = 2  # e.g. age: Male / Female
N_group2 = 5  # e.g. 5 age segments

# group codes
group1 = ["M", "F"]
assert len(group1) == N_group1
group2 = ["12-17", "18-34", "35-49", "50-64", "65+"]
assert len(group2) == N_group2

# generate synthetic data - loop 
for i in xrange(N_test):
    user_id = i
    age_id = group2[user_id % N_group2]
    user_seg = group1[user_id % N_group1] 

    str_fmt = "{},{},{}"  # mimic csv
    record = str_fmt.format(user_id, age_id, user_seg) 
    print record 
