package com.sunk.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

    def main(args: Array[String]): Unit = {
        // 1 准备环境
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark01_RDD_Memory")
        val sc = new SparkContext(sparkConf)

        // 2 创建 RDD - 从文件中创建
        // - path 路径默认以当前环境的根路径为基准，支持绝对路径和相对路径
        // - path 路径可以使具体的文件也可以是一个目录
        // - path 路径支持 '*' 通配符
        // - path 路径支持分布式文件系统路径，如 HDFS
        val rdd = sc.textFile("SparkCore/src/main/resources/input/word.txt")
        rdd.collect().foreach(println)

        // - wholeTextFiles 以整个文件为单位进行读取，读取的结果为元组（文件名，文件内容）
        val rdd2 = sc.wholeTextFiles("SparkCore/src/main/resources/input/word.txt")
        // (file:/D:/Workspace/Code/idea/Spark3.0/SparkCore/src/main/resources/input/word.txt,Hello Spark
        // Hello Scala)
        rdd2.collect().foreach(println)

        // 3 关闭环境
        sc.stop()
    }

}
