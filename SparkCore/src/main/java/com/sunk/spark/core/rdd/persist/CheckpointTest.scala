package com.sunk.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object CheckpointTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)
        sc.setCheckpointDir("SparkCore/src/main/resources/output/checkpoint")

        // 通用的部分
        val list = List("Hello Spark")
        val rdd = sc.makeRDD(list)
        val flatRdd = rdd.flatMap(_.split(" "))
        val mapRdd = flatRdd.map(line => {
            println(s"MAP TASK : $line")
            (line, 1)
        })

        /*
         * checkpoint 需要落盘，需要指定检查点的保存路径，而 cache 操作是保存为临时文件
         * - 一般的保存路径是在 hdfs 中
         * - checkpoint 会独立执行一次任务
         */
        mapRdd.cache()
        mapRdd.checkpoint()

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
