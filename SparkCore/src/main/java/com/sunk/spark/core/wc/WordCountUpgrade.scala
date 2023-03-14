package com.sunk.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountUpgrade {

    def main(args: Array[String]): Unit = {
        // 1 创建 Spark 运行配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 2 创建 Spark 上下文对象
        val sc = new SparkContext(sparkConf)

        // 3 读取文件数据
        val fileRdd: RDD[String] = sc.textFile("SparkCore/src/main/resources/input/word.txt")

        // 4 分词
        val wordRdd: RDD[String] = fileRdd.flatMap(line => line.split(" "))

        // 5 转换数据结构
        val word2CountRdd: RDD[(String, Int)] = wordRdd.map(word => (word, 1))

        // 6 分组
        val groupRdd: RDD[(String, Iterable[(String, Int)])] = word2CountRdd.groupBy(_._1)

        // 7 统计
        val result: RDD[(String, Int)] = groupRdd.map(t => {
            (t._1, t._2.size)
        })

        // 8 采集结果到内存
        val array: Array[(String, Int)] = result.collect()

        // 9 打印结果
        println(array.mkString("Array(", ", ", ")"))

        // 10 关闭 Spark 连接
        sc.stop()
    }

}
