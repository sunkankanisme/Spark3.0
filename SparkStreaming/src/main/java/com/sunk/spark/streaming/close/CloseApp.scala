package com.sunk.spark.streaming.close

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.util.concurrent.Executors

object CloseApp {

    def main(args: Array[String]): Unit = {
        // 从检查点恢复 ssc
        val ckPath = "C:\\Data\\TEMP\\spark\\ck\\state_test"

        val ssc = StreamingContext.getActiveOrCreate(ckPath, () => {
            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
            new StreamingContext(sparkConf, Seconds(3))
        })
        ssc.checkpoint(ckPath)


        val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val wordStream: DStream[String] = lineStream.flatMap(_.split(" "))

        val wordToOneStream: DStream[(String, Int)] = wordStream.map((_, 1))

        val resStream: DStream[(String, Int)] = wordToOneStream.reduceByKey(_ + _)

        resStream.print()

        // 启动采集器
        ssc.start()

        Executors.newSingleThreadExecutor().submit(new Runnable {
            override def run(): Unit = {
                // 例如使用 Redis 做一个标记
                var flag = true

                while (flag) {
                    // 根据 Redis 的值来更新
                    flag = false
                }

                if (ssc.getState() == StreamingContextState.ACTIVE) {
                    // 接收器不再接收数据，将当前的数据处理完了再关闭 app
                    ssc.stop(stopSparkContext = true, stopGracefully = true)
                }
            }
        })

        // 等待采集器的关闭
        ssc.awaitTermination()
    }

}
