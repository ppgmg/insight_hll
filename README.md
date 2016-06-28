# insight_hll

Exploring `hyperloglog` and `Bloom filter` implementations to count unique elements in a data stream on a distributed system.

Info on SlideShare:  [k-lo.ca](http://k-lo.ca)

Video with narration:  [YouTube](https://youtu.be/ZRCLZ3aIaVU)

---

## About Hyperloglog

HyperLogLog is an algorithm used to count the number of unique elements in a data set. Typical counting structures provide exact counts at varying degrees of efficiency, but all generally require memory in proportion to the number of unique elements to be counted. These memory requirements can become onerous as data sets get very large. 

In contrast, probabilistic structures use a (typically much smaller) fixed amount of memory (tunable depending on the desired error rate), at the cost of obtaining a count that is approximate.  For example, counts of billions or trillions of unique items can be tracked using a Hyperloglog structure that requires only several KB of memory, with a typical error rate of 2%. 

By hashing elements to be counted, uniformly distributed random numbers with varying lengths of leading zeros in their binary representations can be generated. In general, the maximum length of leading zeros in these binary representations can be used to estimate the count of unique items. To minimize variance, however, rather than computing a single such maximum length for the entire data set, data is first split into numerous subsets; the maximum length of leading zeros for each subset can be computed, and the resultant values are combined using a harmonic mean to obtain a final value that is converted to an overall estimate.

[For further details: Wikipedia](https://en.wikipedia.org/wiki/HyperLogLog)

## About Bloom Filters

A Bloom filter is a relatively better-known probabilistic data structure (having been conceived in 1970) used to test whether an element is a member of a set. This test can be performed very quickly, at the expense of possibly obtaining a false positive.

[For further details: Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter)

---

## Example implementation

Requirements:  Hadoop, Spark, Zookeeper, Kafka

### Create a topic (e.g. site_views) where data will be streamed to.

We choose 4 partitions to increase parallelized reads and writes.

```
any-node:~$ /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic site_views --partitions 4 --replication-factor 2
```
Optionally check topics:

```
any-node:~$ /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
```

### Compile files in insight_hll directory

After cloning repository onto master node, from the master_files directory:

```
root-node:~/insight_hll/master_files$ sbt assembly
root-node:~/insight_hll/master_files$ sbt package
```

### Run spark script on master node (.scala file compiled)

To run with messages to standard output (replace 172-31-0-69 using info from private DNS for master node):

```
root-node:~/insight_hll/master_files$ spark-submit --class DataStreaming --master spark://ip-172-31-0-69:7077 --jars target/scala-2.10/site_data-assembly-1.0.jar target/scala-2.10/site_data_2.10-1.0.jar
```

OR pipe output to a file (replace 172-31-0-69 using info from private DNS address for master node):

```
root-node:~/insight_hll/master_files$ spark-submit --class DataStreaming --master spark://ip-172-31-0-69:7077 --jars target/scala-2.10/site_data-assembly-1.0.jar target/scala-2.10/site_data_2.10-1.0.jar > myoutputfile
```

### Submit simulated data in testfile to Kafka

Create custom files by running test.py (can be done from master node or elsewhere) and piping output to a file (replace 52.201.165.163 with information from public DNS for master node, and 100000 with number of unique records to be generated):

```
root-node:~/insight_hll$ python test.py 52.201.165.163 1 100000 > 100k.test
```

OR use provided test file.  

Then from same machine (configured to send messages to Kafka for ingestion), transmit contents of file:

```
root-node:~/insight_hll$ /usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic site_views < 100k.test 
```

### Tracking progress

With the job running, diagnostics can be viewed from a browser (replace ec2-52-201-165-163.compute-1 with information from public DNS for master node):

```
http://ec2-52-201-165-163.compute-1.amazonaws.com:4040/streaming/
```

