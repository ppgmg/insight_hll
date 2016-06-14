// site_data.scala

// This Spark Streaming code reads data streamed from Kafka partitions
// and processes the data to compute the total number of unique
// User IDs using the Hyperloglog algorithm as provided by
// Twitter Algebird.

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import com.twitter.algebird._
import com.twitter.algebird.Operators._
import com.twitter.algebird.HyperLogLog._
import org.apache.commons.io.Charsets


object DataStreaming {

  def main(args: Array[String]) {

    // configure Kafka parameters
    val brokers = "ec2-52-201-165-163.compute-1.amazonaws.com:9092" // master
    val topics = "site_views"
    val topicsSet = topics.split(",").toSet

    // configure batch intervals and create context
    val interval = 1  // seconds
    val buildname = "site_data"  // name of build file
    val sparkConf = new SparkConf().setAppName(buildname)
    val ssc = new StreamingContext(sparkConf, Seconds(interval))

    // create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // initialize Hyperloglog counters 
    val globalHll = new HyperLogLogMonoid(12)
    var all_ids = globalHll.zero  // HLL counter for all user IDs
    var all_males = globalHll.zero  // HLL counter for all males
    var all_age2 = globalHll.zero  // HLL counter for all users in age seg

    // initialize counters to store set of counted elements
    // note this implementation assumes we can convert user ID to integer 
    var userSet: Set[Int] = Set()
    var malesSet: Set[Int] = Set()
    var age2Set: Set[Int] = Set()
 
    // control what to output
    val approxcounts = true
    val exactcounts = true 
  
    // process each data record 
    messages.foreachRDD { rdd =>

        // show read data as Data Frame to facilitate testing
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val lines = rdd.map(_._2)
        val ticksDF = lines.map( x => {
                                  val tokens = x.split(";")
                                  Tick(tokens(1), 1)}).toDF()
        val ticks_per_source_DF = ticksDF.groupBy("source").agg("count" -> "sum") 
        ticks_per_source_DF.show()

        // hyperloglog counting

        val ticks_pre = lines.map( x => {
                                 val tokens2 = x.split(";")
                                 (tokens2(1).toInt, (tokens2(2), tokens2(3))) }
                                  ).persist()

        val ids = ticks_pre.keys
        val males_filter = ticks_pre.filter ( x => x._2._2 == "M")
        val males = males_filter.keys
        val age2_filter = ticks_pre.filter (x => x._2._1 == "18-34")
        val age2 = age2_filter.keys

        if (approxcounts){
            //  1. count ids
            val approxids_pre = ids.mapPartitions(ids => {
                                    val hll = new HyperLogLogMonoid(12)
                                    ids.map(id => hll(id))
                                     })

            if (approxids_pre.count()!=0){
                val approxids_hll = approxids_pre.reduce(_ + _)
                all_ids += approxids_hll  // update global
                println("OUT: Approx distinct ids this batch: %d".format(approxids_hll.estimatedSize.toInt))
                println("OUT: Approx distinct ids overall: %d".format(globalHll.estimateSize(all_ids).toInt))
            }else{
                println("OUT: Approx distinct ids this batch: 0")
                println("OUT: Approx distinct ids overall: %d".format(globalHll.estimateSize(all_ids).toInt))
            }

            // 2. count males
            val approxmales_pre = males.mapPartitions(ids => {
                                    val hll = new HyperLogLogMonoid(12)
                                    ids.map(id => hll(id))
                                     })

            if (approxmales_pre.count()!=0){
                val approxmales_hll = approxmales_pre.reduce(_ + _)
                all_males += approxmales_hll  // update global
                println("OUT: Approx distinct males this batch: %d".format(approxmales_hll.estimatedSize.toInt))
                println("OUT: Approx distinct males overall: %d".format(globalHll.estimateSize(all_males).toInt))
            }else{
                println("OUT: Approx distinct males this batch: 0")
                println("OUT: Approx distinct males overall: %d".format(globalHll.estimateSize(all_males).toInt))
            }                               

            // 3. count ids belonging to specific group            
            val approxage2_pre = age2.mapPartitions(ids => {
                                    val hll = new HyperLogLogMonoid(12)
                                    ids.map(id => hll(id))
                                     })

            if (approxage2_pre.count()!=0){
                val approxage2_hll = approxage2_pre.reduce(_ + _)
                all_age2 += approxage2_hll  // update global
                println("OUT: Approx distinct 18-34s this batch: %d".format(approxage2_hll.estimatedSize.toInt))
                println("OUT: Approx distinct 18-34s overall: %d".format(globalHll.estimateSize(all_age2).toInt))
            }else{
                println("OUT: Approx distinct 18-34s this batch: 0")
                println("OUT: Approx distinct 18-34s overall: %d".format(globalHll.estimateSize(all_age2).toInt))
            }

        }


        // calculate exact counts if desired, using sets
        if (exactcounts){
            //  1. count ids
            if (ids.count()!=0){
                val totalids = ids.map(id => Set(id)).reduce(_ ++ _)
                userSet += totalids
                println("OUT: Exact distinct ids this batch: %d".format(totalids.size)) 
                println("OUT: Exact distinct ids overall: %d".format(userSet.size))
            }else{
                println("OUT: Exact distinct ids this batch: 0")
                println("OUT: Exact distinct ids overall: %d".format(userSet.size))
            }

            //  2. count males
            if (males.count()!=0){
                val totalmales = males.map(id => Set(id)).reduce(_ ++ _)
                malesSet += totalmales
                println("OUT: Exact distinct males this batch: %d".format(totalmales.size))
                println("OUT: Exact distinct males overall: %d".format(malesSet.size))
            }else{
                println("OUT: Exact distinct males this batch: 0")
                println("OUT: Exact distinct males overall: %d".format(malesSet.size))
            }

            // 3. count ids belonging to specific group
            if (age2.count()!=0){ 
                val totalage2 = age2.map(id => Set(id)).reduce(_ ++ _)
                age2Set += totalage2
                println("OUT: Exact distinct 18-34s this batch: %d".format(totalage2.size))
                println("OUT: Exact distinct 18-34s overall: %d".format(age2Set.size))
            }else{
                println("OUT: Exact distinct 18-34s this batch: 0")
                println("OUT: Exact distinct 18-34s overall: %d".format(age2Set.size))
            }

        }

    }
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

case class Tick(source: String, count: Int)

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

 