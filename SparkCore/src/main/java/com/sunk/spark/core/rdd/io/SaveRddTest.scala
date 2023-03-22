package com.sunk.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SaveRddTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[(String, String)] = sc.makeRDD(List(
            ("NBA", "xxx"),
            ("cNBA", "xxx"),
            ("wNBA", "xxx"),
            ("NBA", "xxx")
        ))

        /*
         * 存储数据
         */
        rdd.cache()
        rdd.saveAsTextFile("SparkCore/src/main/resources/output/rdd_io/text")
        rdd.saveAsObjectFile("SparkCore/src/main/resources/output/rdd_io/object")
        rdd.saveAsSequenceFile("SparkCore/src/main/resources/output/rdd_io/sequence")
        rdd.unpersist()

        sc.stop()
    }

}
