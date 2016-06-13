# insight_hll
exploring hyperloglog implementations to count uniques

## Create a topic (e.g. ad_views) where data will be streamed to.
We choose 4 partitions to increase parallelized reads and writes.
```
any-node:~$ /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic ad_views --partitions 4 --replication-factor 2
```

## Simulate data from local machine
python kafka_prod_test.py 52.201.165.163 1
