package com.sunk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

    def main(args: Array[String]): Unit = {
        // 1 准备环境
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark01_RDD_Memory")
        val sc = new SparkContext(sparkConf)

        // 2 创建 RDD - 从内存中的集合
        // 2.1 parallelize
        val rdd1 = sc.parallelize(List(1, 2, 3, 4))
        rdd1.collect().foreach(println)

        // 2.2 makeRDD
        val rdd2 = sc.makeRDD(Seq(5, 6, 7, 8))
        rdd2.collect().foreach(println)

        // 3 关闭环境
        sc.stop()
    }

}
