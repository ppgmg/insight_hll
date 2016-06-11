// voting_data.scala

// read data streamed from Kafka partitions and process in Spark Streaming

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import com.twitter.algebird._ 
import com.twitter.algebird.Operators._
import com.twitter.algebird.HyperLogLog.int2Bytes
import org.apache.commons.io.Charsets

object VotingDataStreaming {

  def main(args: Array[String]) {

    val brokers = "ec2-52-201-165-163.compute-1.amazonaws.com:9092"
    val topics = "vote_data"
    val topicsSet = topics.split(",").toSet

    // Create context with 1 second batch interval
    val sparkConf = new SparkConf().setAppName("voting_data")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Get the lines and show results
    messages.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val lines = rdd.map(_._2)
        val ticksDF = lines.map( x => {
                                  val tokens = x.split(";")
                                  Tick(tokens(2), tokens(3).toDouble)}).toDF()
        val ticks_per_source_DF = ticksDF.groupBy("source")
                                .agg("count" -> "sum")
                                .orderBy("source").persist()

        ticks_per_source_DF.show()

        // get count of distinct keys
        val uniques = ticks_per_source_DF.count()
        println("Unique count of keys: %s".format(uniques))

        // hyperLogLog

        lazy val hllMonoid = new HyperLogLogMonoid(bits = 12)
       
        val ticks = lines.map( x => {
                                 val tokens2 = x.split(";")
                                 tokens2(2).toInt } ) 
        println("As list: %s".format(ticks.collect().mkString(",")))

        // ticks.show()
        val tlist = ticks.collect()
        val ticksnext = tlist.map { x => hllMonoid(x) }
        val combined = hllMonoid.sum(ticksnext)
        val approxsize = hllMonoid.sizeOf(combined)

        println("Unique count of keys via HLL: %s".format(approxsize))
 
        //val ticksaslist = ticks.rdd.map(r => r(0))
        //ticksaslist.foreach(println)

        // val combinedHLL = hllMonoid.sum(ticks(1))
        // val uniques2 = hllMonoid.sizeOf(combinedHLL)

 
        // val ticks = lines.map( x => {
        //                         val tokens2 = x.split(";")
        //                         val key = tokens2(2).toInt
        //                         val uniquesHll = hllMonoid(tokens2(2).getBytes(Charsets.UTF_8)) 
        //                         (key, uniquesHll) } ) 

        // val combined = ticks.reduceByKey
        // ticks.collect()
        // val ticksaslist = ticks.flatMap(y => y).collect() 
        //for (element <- ticks){
        //    print(element)
        //} 
        //val hll = ticks.map { hllMonoid.create(_) }
        //val combinedHLL = hllMonoid.sum(hll)

        //val uniques2 = hllMonoid.sizeOf(combinedHLL) 
        //println("Unique count of keys via HLL: %s".format(uniques2))
 
        // TODO: val runningCounts = ticks_per_source_DF.updateStateByKey[Int](updateFunction _)  
        // TODO runningCounts.show() 
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

case class Tick(source: String, count: Double)

case class ID(k: Int)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
