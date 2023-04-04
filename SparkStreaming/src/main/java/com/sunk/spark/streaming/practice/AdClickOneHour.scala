package com.sunk.spark.streaming.practice

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AdClickOneHour {

    def main(args: Array[String]): Unit = {
        val ckPath = "C:\\Data\\TEMP\\spark\\ck\\AdClickOneHour"
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AdClickOneHour")
        val ssc = new StreamingContext(sparkConf, Seconds(1))

        ssc.checkpoint(ckPath)

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "group_spark_test",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )

        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("spark_streaming_topic"), kafkaPara))

        val adClickStream: DStream[AdClick] = kafkaStream.map(line => {
            val splits = line.value().split(",")
            AdClick(splits(0), splits(1), splits(2), splits(3), splits(4))
        })

        /*
         * 最近一分钟，每 10s 计算一次
         */
        val reduceStream = adClickStream.map(data => {
            // 将 ts 按分钟划分
            val newTs = data.ts.toLong / 1000 / 10 * 10000
            (newTs, 1)
        }).reduceByKeyAndWindow((x: Int, y: Int) => {
            x + y
        }, Seconds(60), Seconds(10))

        reduceStream.print()


        ssc.start()
        ssc.awaitTermination()
    }

    case class AdClick(ts: String, area: String, city: String, user: String, adId: String)

}
