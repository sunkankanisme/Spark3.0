package com.sunk.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Transform 案例实操
 */
object TransformDemo {
    def main(args: Array[String]): Unit = {
        // 1 创建执行环境
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("TransformDemo")
        val sc = new SparkContext(sparkConf)

        // 2 加载原始数据并且转换数据格式
        val textRdd = sc.textFile("SparkCore/src/main/resources/data/agent.log")
        val mapRdd: RDD[((String, String), Int)] = textRdd
                .map(line => {
                    val strings = line.split(" ")
                    ((strings(1), strings(4)), 1)
                })

        // 3 按照 省份 + 广告 统计
        val reduceByKeyRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)

        // 4 将聚合的结果进行结构的转换，并按照省份分组
        val groupByRdd: RDD[(String, Iterable[(String, String, Int)])] = reduceByKeyRdd.map(t => (t._1._1, t._1._2, t._2)).groupBy(_._1)

        // 5 排序输出结果(降序)
        val resultRdd: RDD[(String, String, Int)] = groupByRdd.flatMap(t => {
            val list: List[(String, String, Int)] = t._2.toList
            list.sortBy(_._3).reverse.take(3)
        })

        // 6 采集打印输出
        resultRdd.collect().foreach(println)

        sc.stop()
    }
}
