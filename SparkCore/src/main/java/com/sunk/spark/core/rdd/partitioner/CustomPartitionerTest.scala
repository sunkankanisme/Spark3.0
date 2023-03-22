package com.sunk.spark.core.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object CustomPartitionerTest {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        val rdd: RDD[(String, String)] = sc.makeRDD(List(
            ("NBA", "xxx"),
            ("cNBA", "xxx"),
            ("wNBA", "xxx"),
            ("NBA", "xxx")
        ))

        /*
         * 需求：将不同俱乐部的消息发送到不同的分区中
         */
        val partRDD = rdd.partitionBy(new MyPartitioner)

        partRDD.foreachPartition(println)

        sc.stop()
    }

    /*
     * 自定义分区器
     * - 继承 Partitioner 类
     * - 重写方法
     */
    class MyPartitioner extends Partitioner {
        // 指定分区数量
        override def numPartitions: Int = 3

        // 根据数据的 key 获取分区号的方式，从 0 开始
        override def getPartition(key: Any): Int = {
            if (key == "NBA") {
                0
            } else if (key == "cNBA") {
                1
            } else {
                2
            }
        }
    }

}
