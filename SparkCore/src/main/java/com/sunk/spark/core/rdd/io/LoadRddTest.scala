package com.sunk.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LoadRddTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        // text
        val rdd1 = sc.textFile("SparkCore/src/main/resources/output/rdd_io/text")
        // (NBA,xxx),(cNBA,xxx),(wNBA,xxx),(NBA,xxx)
        println(rdd1.collect().mkString(","))

        // object
        val rdd2 = sc.objectFile[(String, String)]("SparkCore/src/main/resources/output/rdd_io/object")
        // (NBA,xxx),(cNBA,xxx),(wNBA,xxx),(NBA,xxx)
        println(rdd2.collect().mkString(","))

        // sequence
        val rdd3 = sc.sequenceFile[String, String]("SparkCore/src/main/resources/output/rdd_io/sequence")
        // (NBA,xxx),(cNBA,xxx),(wNBA,xxx),(NBA,xxx)
        println(rdd3.collect().mkString(","))


        sc.stop()
    }

}
