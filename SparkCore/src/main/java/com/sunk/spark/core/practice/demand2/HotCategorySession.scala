package com.sunk.spark.core.practice.demand2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategorySession {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        // 1 读取原始日志
        val dataRdd: RDD[String] = sc.textFile("SparkCore/src/main/resources/data/user_visit_action.txt")
        dataRdd.cache()

        val top10IDs: Array[String] = top10Category(dataRdd)

        // 2 过滤原始数据，保留点击和前 10 品类 ID
        val filterRdd = dataRdd.filter(line => {
            val strings = line.split("_")
            if (strings(6) != "-1" && top10IDs.contains(strings(6))) {
                true
            } else {
                false
            }
        })

        // 3 根据品类ID + SessionId 做统计
        val reduceRdd: RDD[((String, String), Int)] = filterRdd.map(action => {
            val strings = action.split("_")

            val sessionId = strings(2)
            val categoryId = strings(6)

            ((categoryId, sessionId), 1)
        }).reduceByKey(_ + _)

        // 4 将统计结果进行结构的转换
        // ((品类ID，会话ID), sum) => (品类ID， (会话ID， sum))
        val mapRdd = reduceRdd.map {
            case ((cid, sid), sum) =>
                (cid, (sid, sum))
        }

        // 5 将相同的品类分组
        val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupByKey()

        // 6 按照数量进行排序，取前10
        val result: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))

        result.foreach(println)

        sc.stop()
    }

    def top10Category(dataRdd: RDD[String]): Array[String] = {

        // 2 将数据转换结构
        // - 点击：(品类ID, (1,0,0))
        // - 下单：(品类ID, (0,1,0))
        // - 支付：(品类ID, (0,0,1))
        val flatRdd = dataRdd.flatMap(line => {
            val strings = line.split("_")

            if (strings(6) != "-1") {
                // 点击
                List((strings(6), (1, 0, 0)))
            } else if (strings(8) != "null") {
                // 下单
                strings(8).split(",").map(id => (id, (0, 1, 0)))
            } else if (strings(10) != "null") {
                // 支付
                strings(10).split(",").map(id => (id, (0, 0, 1)))
            } else {
                Nil
            }
        })

        // 3 数据聚合
        // - (品类ID, (点击数量, 下单数量, 支付数量))
        val resultRdd: RDD[(String, (Int, Int, Int))] = flatRdd.reduceByKey((t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
        })

        resultRdd.sortBy(_._2, ascending = false).take(10).map(_._1)
    }

}
