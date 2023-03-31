package com.sunk.spark.streaming.dstream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object CustomSource {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val dstream = ssc.receiverStream(new MyReceiver())
        dstream.print()

        ssc.start()
        ssc.awaitTermination()
    }

    /*
     * 自定义数据采集器
     * 1 继承 Receiver 定义泛型
     * 2 传入存储级别参数
     * 3 重写方法
     */
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
        private var flag = true

        override def onStart(): Unit = {
            new Thread(() => {
                while (flag) {
                    Thread.sleep(500)
                    val message = new Random().nextInt(10).toString
                    // 封装数据
                    store(message)
                }
            }).start()
        }

        override def onStop(): Unit = {
            flag = false
        }
    }

}
