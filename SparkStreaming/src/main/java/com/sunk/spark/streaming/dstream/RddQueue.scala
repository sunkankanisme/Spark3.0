package com.sunk.spark.streaming.dstream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RddQueue {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 1 创建 RDD 队列
        val rddQueue = new mutable.Queue[RDD[Int]]()

        // 2 创建 DStream
        val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)

        // 3 处理队列中的 RDD 数据
        val mappedStream = inputStream.map((_, 1))
        val reducedStream = mappedStream.reduceByKey(_ + _)

        // 4 打印结果
        reducedStream.print()

        ssc.start()

        // 5 循环创建并向 RDD 队列中放入 RDD
        for (i <- 1 to 5) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()
    }

}
