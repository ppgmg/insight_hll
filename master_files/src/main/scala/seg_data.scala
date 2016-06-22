// seg_data.scala

// This Spark code reads data from a CSV file and saves instances
// of Bloom Filters for querying during a Spark Streaming application.

// Uses the Bloom Filter structures as provided by Twitter Algebird. 
// After compilation: 
// spark-submit --class genBlooms --jars target/scala-2.10/site_data-assembly-1.0.jar target/scala-2.10/site_data_2.10-1.0.jar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import com.twitter.algebird._
import com.twitter.algebird.Operators._
import com.twitter.algebird.BloomFilter._

object genBlooms {

  def main(args: Array[String]) {

    val buildname = "generateSegs"  // name of build file
    val sparkConf = new SparkConf().setAppName(buildname)
    val sc = new SparkContext(sparkConf)

    // Load input data
    val input = sc.textFile("1Msegs.csv")
    val items = input.map(x => {
                                val tokens = x.split(",")
                                val id = tokens(0)
                                val gender = tokens(2)
                                val age = tokens(1)
                                (id, (gender, age))
                                }).persist()

    val tot = items.count().toInt
    val males = items.filter ( x => x._2._1 == "M").keys
    val group2 = items.filter ( x => x._2._2 == "18-34").keys

    // Generate bloom filter - http://hur.st/bloomfilter?n=1000000&p=0.03
    // 1 million elements to test
    // specific parameters can also be calculated using Algebird functions
    val k = 5  // number of hashes 
    val w = 7298441 // width in bits
 
    val males_bf = males.mapPartitions( ids => {
                        val bfstruct = new BloomFilterMonoid(k, w, 1)
                        ids.map(id => bfstruct.create(id))
                        }) 
    val m_bf = males_bf.reduce(_ ++ _)

    val group2_bf = group2.mapPartitions( ids => {
                        val bfstruct = new BloomFilterMonoid(k, w, 1)
                        ids.map(id => bfstruct.create(id))
                        })
    val age2_bf = group2_bf.reduce(_ ++ _)

    // save bloom filters to file 
    sc.parallelize(List(m_bf)).saveAsObjectFile("malebf")
    sc.parallelize(List(age2_bf)).saveAsObjectFile("age2bf")


    // test reloading
    val reloadmales = sc.objectFile[com.twitter.algebird.BF]("malebf").first()
    val reloadage2 = sc.objectFile[com.twitter.algebird.BF]("age2bf").first()

    var cnt1 = 0
    var cnt2 = 0
    // test membership - bloom filter query
    for (i <- 0 to tot-1) {
       if (reloadmales.contains(i.toString).isTrue){
           cnt1 += 1
       }

       if (reloadage2.contains(i.toString).isTrue){
           cnt2 += 1
       }
    }

    println("total males: %d".format(cnt1))
    println("total 18-34: %d".format(cnt2))

  }
}

 
