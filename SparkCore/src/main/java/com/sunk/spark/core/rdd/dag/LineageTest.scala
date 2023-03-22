package com.sunk.spark.core.rdd.dag

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LineageTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建 SparkContext,该对象是提交 Spark App 的入口
        val sc: SparkContext = new SparkContext(conf)

        val fileRDD: RDD[String] = sc.textFile("SparkCore/src/main/resources/input/word.txt")
        println(fileRDD.toDebugString)

        println("----------------------")
        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
        // toDebugString 打印血缘关系
        println(wordRDD.toDebugString)

        println("----------------------")
        val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
        println(mapRDD.toDebugString)

        println("----------------------")
        val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        println(resultRDD.toDebugString)

        resultRDD.collect()

        sc.stop()
    }

}
