package com.sunk.spark.streaming.transforms

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WithState {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        // 使用有状态操作需要设置检查点
        ssc.checkpoint("C:\\Data\\TEMP\\spark\\ck\\state_test")

        val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val wordToOneStream: DStream[(String, Int)] = socketStream.map((_, 1))

        /*
         * 1 UpdateStateByKey
         *
         * 根据 key 对状态进行更新
         * - seq：相同的 key 的 value 数据
         * - opt：缓冲区内相同的 key 的 value 数据，缓冲区为 Option 类型，第一次访问时没有值
         */
        // val stateDStream: DStream[(String, Int)] = wordToOneStream.updateStateByKey(
        //     (seq: Seq[Int], buff: Option[Int]) => {
        //         val newCount = buff.getOrElse(0) + seq.sum
        //         Option(newCount)
        //     }
        // )
        // stateDStream.print()

        /*
         * 2 WindowOperations
         */
        val windowStream: DStream[(String, Int)] = wordToOneStream.window(Seconds(6), Seconds(6))
        windowStream.reduceByKey(_ + _).print()


        ssc.start()
        ssc.awaitTermination()
    }

}
