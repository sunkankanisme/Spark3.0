package com.sunk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // Seconds(3) - 批次采集周期
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val wordStream: DStream[String] = lineStream.flatMap(_.split(" "))

        val wordToOneStream: DStream[(String, Int)] = wordStream.map((_, 1))

        val resStream: DStream[(String, Int)] = wordToOneStream.reduceByKey(_ + _)

        resStream.print()

        // 启动采集器
        ssc.start()
        // 等待采集器的关闭
        ssc.awaitTermination()
    }

}
