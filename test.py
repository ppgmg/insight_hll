# kafka_prod_test.py
# last revised 13 Jun 2016

# This is a script to randomly generate test data for ingestion by Kafka.
# We model this simulated data as representing interactive site-view data

import random
import sys
# import six  # to address issues between Python versions
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import json


class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol, count):

        # configurable parameters for testing
        kafka_topic = "site_views"
        N_test = int(count)  # number of simulated data items
        N_items_per_user = 1  # total users = N_test / N_items_per_user
        N_group1 = 2  # e.g. age: Male / Female
        N_group2 = 5  # e.g. 5 age segments

        # group codes
        group1 = ["M", "F"]
        assert len(group1) == N_group1
        group2 = ["12-17", "18-34", "35-49", "50-64", "65+"]
        assert len(group2) == N_group2

        # generate synthetic data - loop
        for i in xrange(N_test):
            asset_id = 1  # fixed for testing
            user_id = i / N_items_per_user
            age_id = group2[user_id % N_group2]
            user_seg = group1[user_id % N_group1]
            timestamp = "time" # datetime.now().strftime("%Y%m%d %H%M%S")

            str_fmt = "{};{};{};{};{}"
            message_info = str_fmt.format(asset_id,
                                          user_id,
                                          age_id,
                                          user_seg,
                                          timestamp)

            print message_info

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    count = str(args[3])
    prod.produce_msgs(partition_key, count)

