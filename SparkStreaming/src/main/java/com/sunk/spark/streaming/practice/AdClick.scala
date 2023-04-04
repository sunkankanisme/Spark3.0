package com.sunk.spark.streaming.practice

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.text.SimpleDateFormat
import java.util.Date

object AdClick {

    def main(args: Array[String]): Unit = {
        val ckPath = "C:\\Data\\TEMP\\spark\\ck\\AdClick"
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AdClick")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

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

        val reduceStream = adClickStream.map(data => {
            val day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(data.ts.toLong))

            ((day, data.area, data.city, data.adId), 1)
        }).reduceByKey(_ + _)

        reduceStream.foreachRDD(rdd => {
            rdd.foreachPartition(part => {
                val connection = JdbcUtil.getConnection
                val statement = connection.prepareStatement("insert into area_city_ad_count (dt,area,city,adid,count) values (?,?,?,?,?) on duplicate key update count = count + ?")

                part.foreach {
                    case ((day, area, city, adId), cnt) =>
                        statement.setString(1, day)
                        statement.setString(2, area)
                        statement.setString(3, city)
                        statement.setString(4, adId)
                        statement.setInt(5, cnt)
                        statement.setInt(6, cnt)
                        statement.executeUpdate()
                }

                statement.close()
                connection.close()
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }

    case class AdClick(ts: String, area: String, city: String, user: String, adId: String)

}
