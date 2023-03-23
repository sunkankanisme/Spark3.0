package com.sunk.spark.core.bc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BcTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        val rdd1 = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a", 4),
            ("b", 5),
            ("c", 6)
        ))

        /*
         * 常规做法
         * - 可能导致数据几何增长，并且产生 shuffle 影响性能
         */
        val res1: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        // (a,(1,4)),(b,(2,5)),(c,(3,6))
        println(res1.collect().mkString(","))

        /*
         * 使用 Map 端集合
         */
        // 将 rdd2 封装为集合
        val map: mutable.Map[String, Int] = mutable.Map(
            ("a", 4),
            ("b", 5),
            ("c", 6)
        )

        val res2 = rdd1.map(t => {
            val target = map.getOrElse(t._1, 0)
            (t._1, (t._2, target))
        })

        // (a,(1,4)),(b,(2,5)),(c,(3,6))
        println(res2.collect().mkString(","))


        /*
         * 使用广播变量
         */
        val bc = sc.broadcast(map)

        val res3 = rdd1.map(t => {
            val target = bc.value.getOrElse(t._1, 0)
            (t._1, (t._2, target))
        })

        // (a,(1,4)),(b,(2,5)),(c,(3,6))
        println(res3.collect().mkString(","))


        sc.stop()
    }
}
