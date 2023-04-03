package com.sunk.spark.streaming.practice

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object AdBlacklist {

    def main(args: Array[String]): Unit = {
        // 从检查点恢复 ssc
        val ckPath = "C:\\Data\\TEMP\\spark\\ck\\AdBlacklist"
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AdBlacklist")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //        val ssc = StreamingContext.getActiveOrCreate(ckPath, () => {
        //            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AdBlacklist")
        //            new StreamingContext(sparkConf, Seconds(3))
        //        })

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

        // 0 周期性获取黑名单数据，每一个批次更新一次
        // 1 判断用户是否在黑名单中
        // - 如果不在黑名单中，则进行统计点击数量（每一个周期），如果数量超过阈值，则将用户拉入黑名单中
        // - 如果不在黑名单中，则当天的点击次数更新，如果更新后超过了阈值，则将用户拉入到黑名单中
        val ds = adClickStream.transform(rdd => {
            val blacklistedUsers = ListBuffer[String]()

            // 在此处使用 jdbc 周期性的获取黑名单数据
            val conn = JdbcUtil.getConnection
            val statement = conn.prepareStatement("select userid from spark.black_list")
            val rs = statement.executeQuery()
            while (rs.next()) {
                blacklistedUsers.append(rs.getString(1))
            }

            rs.close()
            statement.close()
            conn.close()

            // 判断用户是否在黑名单中
            val filterRdd = rdd.filter(click => {
                if (blacklistedUsers.contains(click.user)) {
                    false
                } else {
                    true
                }
            })

            // 如果不在黑名单中，则进行统计点击数量（每一个周期），如果数量超过阈值，则将用户拉入黑名单中
            filterRdd.map(click => {
                val day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(click.ts.toLong))
                val user = click.user
                val ad = click.adId

                ((day, user, ad), 1)
            }).reduceByKey(_ + _)
        })

        ds.foreachRDD(rdd => {
            println(s"=== ${System.currentTimeMillis()}, ${rdd.count()} ===")
            rdd.foreach {
                case ((day, user, ad), cnt) =>
                    if (cnt >= 30) {
                        // (每一个周期），如果数量超过阈值，则将用户拉入黑名单中
                        val conn = JdbcUtil.getConnection
                        val statement = conn.prepareStatement("insert into spark.black_list(userid) values (?) ON DUPLICATE KEY UPDATE userid = ?")
                        statement.setString(1, user)
                        statement.setString(2, user)
                        statement.executeUpdate()

                        statement.close()
                        conn.close()
                    } else {
                        // 更新当天的点击次数更新，如果更新后超过了阈值，则将用户拉入到黑名单中
                        // 查询统计表数据，如果存在数据则更新，如果不存在则新增
                        val conn = JdbcUtil.getConnection
                        val statement = conn.prepareStatement("select * from spark.user_ad_count where dt = ? and userid = ? and adid = ?")
                        statement.setString(1, day)
                        statement.setString(2, user)
                        statement.setString(3, ad)
                        val resultSet = statement.executeQuery()
                        if (resultSet.next()) {
                            // 更新数据
                            val statement1 = conn.prepareStatement("update spark.user_ad_count set count = count + ? where dt = ? and userid = ? and adid = ?")
                            statement1.setInt(1, cnt)
                            statement1.setString(2, day)
                            statement1.setString(3, user)
                            statement1.setString(4, ad)
                            statement1.executeUpdate()
                            statement1.close()

                            // 如果更新后超过了阈值，则将用户拉入到黑名单中
                            val statement2 = conn.prepareStatement("select * from spark.user_ad_count where dt = ? " +
                                "and userid = ? and adid = ? and count >= 30")
                            statement2.setString(1, day)
                            statement2.setString(2, user)
                            statement2.setString(3, ad)
                            val resultSet2 = statement2.executeQuery()

                            if (resultSet2.next()) {
                                val statement3 = conn.prepareStatement("insert into spark.black_list(userid) values (?) " +
                                    "ON DUPLICATE KEY UPDATE userid = ?")
                                statement3.setString(1, user)
                                statement3.setString(2, user)
                                statement3.executeUpdate()
                                statement3.close()
                            }

                            statement2.close()
                            resultSet2.close()
                        } else {
                            // 插入数据
                            val statement1 = conn.prepareStatement("insert into spark.user_ad_count(dt,userid,adid,count) values (?,?,?,?)")
                            statement1.setString(1, day)
                            statement1.setString(2, user)
                            statement1.setString(3, ad)
                            statement1.setInt(4, cnt)
                            statement1.executeUpdate()
                            statement1.close()
                        }

                        resultSet.close()
                        statement.close()
                        conn.close()
                    }
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }

    case class AdClick(ts: String, area: String, city: String, user: String, adId: String)

}
