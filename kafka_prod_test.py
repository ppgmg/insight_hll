# kafka_prod_test.py
# last revised 7 Jun 2016

# This is a script to randomly generate test data for ingestion by Kafka
# We model this simulated data as representing interactive voting data
# (e.g. via SMS with phone-ID)

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
        show_id: int
        candidate_id: int  (candidate to receive vote)
        user_id: int
        device_id: int
        timestamp: string
        user_segs: list of strings
        '''

        N_test = 1000  # number of simulated data items
        for i in xrange(N_test):
            show_id = 1  # fixed for testing
            candidate_id = 1  # fixed for testing

            u = 5  # number of distinct users for control
            user_id = i / 5
            device_id = 1  # fixed for testing

            timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
            segs = ["a.M", "a.F"]
            user_segs = segs[user_id % 2]  # random.choice(segs)

            str_fmt = "{};{};{};{};{};{}"
            message_info = str_fmt.format(show_id,
                                          candidate_id,
                                          user_id,
                                          device_id,
                                          timestamp,
                                          user_segs)

            print message_info
            self.producer.send_messages('vote_data',
                                        source_symbol,
                                        message_info)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
