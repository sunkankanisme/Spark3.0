package com.sunk.spark.streaming.dstream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSource {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 1 使用 KafkaUtils 创建流
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
            // 指定消费者组
            ConsumerConfig.GROUP_ID_CONFIG -> "group_spark_test",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )

        /*
         * - ssc: StreamingContext
         * - LocationStrategies.PreferConsistent: 采集的节点和计算的节点如何分配
         * - ConsumerStrategies.Subscribe[String, String]
         *      (Set("spark_topic"), kafkaPara)): 指定主题和 Kafka 配置
         */
        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("spark_topic"), kafkaPara))

        // 获取值
        kafkaStream.map(_.value()).print()

        ssc.start()
        ssc.awaitTermination()
    }

}
