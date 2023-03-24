package com.sunk.spark.core.practice.demand1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotCategory3 {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        // 1 读取原始日志
        val dataRdd: RDD[String] = sc.textFile("SparkCore/src/main/resources/data/user_visit_action.txt")
        dataRdd.cache()

        /*
         * TODO 存在大量的 ReduceByKey
         */

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
        val resultRdd = flatRdd.reduceByKey((t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
        })

        // 4 统计结果排序输出
        // - 元组的排序是按元素依次排序
        resultRdd.sortBy(_._2, ascending = false).take(10).foreach(println)

        dataRdd.unpersist()
        sc.stop()
    }

}
