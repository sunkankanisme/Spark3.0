package com.sunk.spark.streaming.practice

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

/*
 * 生成模拟数据
 */
object MockData {

    def main(args: Array[String]): Unit = {
        // 时间戳，省份，城市，用户，广告
        // timestamp province city userid adid

        val prop = new Properties()
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092,hadoop102:9092,hadoop103:9092")
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        val kafkaProducer = new KafkaProducer[String, String](prop)

        val topic = "spark_streaming_topic"

        while (true) {
            println("mocking ...")

            for (data <- mockData()) {
                kafkaProducer.send(new ProducerRecord[String, String](topic, data))
            }
            Thread.sleep(300)
        }
    }

    private def mockData(): ListBuffer[String] = {
        val list: ListBuffer[String] = ListBuffer[String]()
        val random = new Random()

        val areaList = ListBuffer[String]("华中", "华东", "华南", "华北")
        val cityList = ListBuffer[String]("北京", "上海", "广州", "深圳")

        for (i <- 1 to random.nextInt(50)) {
            val userId = random.nextInt(6)
            val adId = random.nextInt(6)
            list.append(s"${System.currentTimeMillis()},${areaList(random.nextInt(4))},${cityList(random.nextInt(4))},$userId,$adId")
        }

        list
    }

}
