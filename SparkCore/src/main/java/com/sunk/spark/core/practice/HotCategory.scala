package com.sunk.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategory {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        // 1 读取原始日志
        val dataRdd: RDD[String] = sc.textFile("SparkCore/src/main/resources/data/user_visit_action.txt")
        dataRdd.cache()

        // 2 统计品类的点击数量 => (id, cnt)
        val clickActionRdd: RDD[String] = dataRdd.filter(_.split("_")(6) != "-1")
        val clickCountRdd: RDD[(String, Int)] = clickActionRdd.map(line => (line.split("_")(6), 1)).reduceByKey(_ + _)

        // 3 统计品类的下单数量 => (id, cnt)
        val orderActionRdd: RDD[String] = dataRdd.filter(_.split("_")(8) != "null")
        val orderCountRdd: RDD[(String, Int)] = orderActionRdd.flatMap(line => {
            val strings = line.split("_")
            // 拆分多个
            val cids = strings(8).split(",")
            cids.map(id => (id, 1))
        }).reduceByKey(_ + _)

        // 4 统计品类的支付数量 => (id, cnt)
        val payActionRdd: RDD[String] = dataRdd.filter(_.split("_")(10) != "null")
        val payCountRdd: RDD[(String, Int)] = payActionRdd.flatMap(line => {
            val strings = line.split("_")
            // 拆分多个
            val cids = strings(10).split(",")
            cids.map(id => (id, 1))
        }).reduceByKey(_ + _)

        // 5 对品类数据进行排序并且取前 10
        // 5.1 联合数据
        val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRdd.cogroup(orderCountRdd, payCountRdd)

        val mapRdd = cogroupRdd.mapValues(t3 => {
            (t3._1.toList.head, t3._2.toList.head, t3._3.toList.head)
        })

        // 5.2 按照点击、下单、支付进行排序
        val result: Array[(String, (Int, Int, Int))] = mapRdd.sortBy(_._2, ascending = false).take(10)

        // 5.3 先按照点击数排名, 靠前的就排名高;如果点击数相同, 再比较下单数，下单数再相同,就比较支付数

        // 6 输出打印
        result.foreach(println)

        dataRdd.unpersist()
        sc.stop()
    }

}
