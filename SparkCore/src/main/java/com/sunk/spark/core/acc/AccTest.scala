package com.sunk.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        // reduce: 包括分区内的计算和分区间的计算
        // val i = rdd.reduce(_ + _)
        // println(i)

        // 直接统计（存在问题，无法累加）
        var sum = 0
        rdd.foreach(i => sum = sum + i)
        // 0
        println(sum)

        /*
         * 使用系统累加器
         */
        val sumAccumulator = sc.longAccumulator("sum")
        rdd.foreach(i => sumAccumulator.add(i))
        // 10
        println(sumAccumulator.value)

        sc.longAccumulator("long_acc")
        sc.doubleAccumulator("double_acc")
        sc.collectionAccumulator("list_acc")


        /*
         * 使用自定义累加器
         */
        // 注册累加器
        val wcAcc = new WordCountAccumulator()
        sc.register(wcAcc, "wc_acc")
        // 使用累加器
        rdd.foreach(i => wcAcc.add(i + ""))
        // 获取累加器的值
        // Map(2 -> 1, 1 -> 1, 4 -> 1, 3 -> 1)
        println(wcAcc.value)

        sc.stop()
    }

    /*
     * 自定义累加器
     * - IN: 输入的类型 String
     * - OUT: 输出的类型 String，Long
     */
    private class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

        var map: mutable.Map[String, Long] = mutable.Map()

        // 判断累加器是否为初始状态
        override def isZero: Boolean = map.isEmpty

        // 复制累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new WordCountAccumulator

        // 重置累加器
        override def reset(): Unit = map.clear()

        // 向累加器中增加数据 (In)
        override def add(word: String): Unit = {
            val newCount = map.getOrElse(word, 0L) + 1
            map.update(word, newCount)
        }

        // Driver 合并多个累加器的方式
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            other.value.foreach {
                case (word, count) =>
                    val newCount = map.getOrElse(word, 0L) + count
                    map.update(word, newCount)
            }
        }

        // 返回累加器的结果 (Out)
        override def value: mutable.Map[String, Long] = map
    }

}
