# kafka_prod_test.py
# last revised 13 Jun 2016

# This is a script to randomly generate test data for ingestion by Kafka.
# We model this simulated data as representing interactive ad-view data

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

    def produce_msgs(self, source_symbol):
        '''
        Basic Schema for Messages:
        asset_id: int  # e.g. ad viewed
        user_id: int
        age_id: string
        user_seg: string 
        timestamp: string
        '''
 
        # configurable parameters for testing
        kafka_topic = "ad_views"
        N_test = 100000  # number of simulated data items
        N_items_per_user = 5  # total users = N_test / N_items_per_user
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
            timestamp = datetime.now().strftime("%Y%m%d %H%M%S")

            str_fmt = "{};{};{};{};{}"
            message_info = str_fmt.format(asset_id,
                                          user_id,
                                          age_id,
                                          user_seg,
                                          timestamp)

            print message_info
            self.producer.send_messages(kafka_topic,
                                        source_symbol,
                                        message_info)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
