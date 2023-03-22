package com.sunk.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        // 通用的部分
        val list = List("Hello Spark", "Hello Scala")
        val rdd = sc.makeRDD(list)
        val flatRdd = rdd.flatMap(_.split(" "))
        val mapRdd = flatRdd.map(line => {
            println(s"MAP TASK $line")
            (line, 1)
        })

        /*
         * 注意不缓存 RDD 的情况下，即使复用 RDD 对象也无法提高效率
         */
        // mapRdd.cache()
        mapRdd.persist(StorageLevel.DISK_ONLY)


        // 功能一
        val reduceRdd = mapRdd.reduceByKey(_ + _)
        reduceRdd.collect().foreach(println)

        println("--------------------")

        // 功能二
        val groupRdd = mapRdd.groupByKey()
        groupRdd.collect().foreach(println)

        sc.stop()
    }

}
