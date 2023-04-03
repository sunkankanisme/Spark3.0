package com.sunk.spark.streaming.transforms

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NoState {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        /*
         * 1 Transform
         */
        // 此处代码在 Driver 端执行
        // val transformRdd: DStream[(String, Int)] = socketStream.transform(rdd => {
        //     // 此处代码在 Driver 端执行，但是对于每一个批次是独立的
        //     rdd.map(line => {
        //         // 此处代码在 Executor 端执行
        //         (line, 1)
        //     })
        // })

        /*
         * 2 join
         */
        val socketStream8899: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8899)

        val stream9999 = socketStream.map((_, 9999))
        val stream8899 = socketStream8899.map((_, 8899))

        val joinStream: DStream[(String, (Int, Int))] = stream8899.join(stream9999)
        joinStream.print()

        ssc.start()
        ssc.awaitTermination()
    }

}
