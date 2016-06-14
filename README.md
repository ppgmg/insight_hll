# insight_hll

Exploring `hyperloglog` implementations to count unique elements in a data stream on a distributed system.

### Create a topic (e.g. site_views) where data will be streamed to.
We choose 4 partitions to increase parallelized reads and writes.
```
any-node:~$ /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic site_views --partitions 4 --replication-factor 2
```
Optionally check topics
```
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
```

### Compile files in directory (if not already done)
```
sbt assembly
sbt package
```

### Run spark script on master (.scala file compiled)
```
spark-submit --class DataStreaming --master spark://ip-172-31-0-69:7077 --jars target/scala-2.10/site_data-assembly-1.0.jar target/scala-2.10/site_data_2.10-1.0.jar
```

### Submit simulated data in testfile to Kafka
```
/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic site_views < testfile
```
