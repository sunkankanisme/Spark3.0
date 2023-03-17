package com.sunk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Para {

    def main(args: Array[String]): Unit = {
        // 1 准备环境
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark01_RDD_Memory")
        val sc = new SparkContext(sparkConf)

        // 2 内存读取
        val rdd2 = sc.makeRDD(Seq(5, 6, 7, 8), numSlices = 4)
        // === 0
        // === 1
        // === 2
        // === 3
        rdd2.partitions.foreach(p => println("=== " + p.index))
        rdd2.collect().foreach(println)

        // 3 文件读取
        // - minPartitions: 最小分区数量，默认值 math.min(defaultParallelism, 2)
        // - 真正的分区数量可能比 minPartitions 大
        // - Spark 读取文件使用的是 Hadoop 的读取方式
        val filePath = "SparkCore/src/main/resources/input/word.txt"
        val rdd3 = sc.textFile(filePath, minPartitions = 2)
        rdd3.partitions.foreach(p => println("=== " + p.index))
        rdd3.collect().foreach(println)

        // 4 关闭环境
        sc.stop()
    }

}
