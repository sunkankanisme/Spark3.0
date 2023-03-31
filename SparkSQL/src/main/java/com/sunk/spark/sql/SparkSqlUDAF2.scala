package com.sunk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

object SparkSqlUDAF2 {

    def main(args: Array[String]): Unit = {
        // 创建环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("SQL")
        // .enableHiveSupport()
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val df = spark.read.json("SparkCore/src/main/resources/data/test.json")

        df.createOrReplaceTempView("user")

        // 使用 udaf 计算平均值
        spark.udf.register("avg_age", functions.udaf(new MyUDAF))
        spark.sql("select avg_age(age) from user").show

        spark.close()
    }

    /*
     * 计算年龄的平均值
     * 1 继承 org.apache.spark.sql.expressions.Aggregator 类
     *      - IN：输入数据类型
     *      - BUF：中间缓冲数据类型
     *      - OUT：输出数据类型
     * 2 重复方法 x6
     */
    class MyUDAF extends Aggregator[Long, (Long, Long), Long] {
        // 初始化缓冲器方法
        override def zero: (Long, Long) = (0, 0)

        // 根据输入数据，累加中间结果（更新缓冲区）
        override def reduce(b: (Long, Long), a: Long): (Long, Long) = {
            (b._1 + a, b._2 + 1)
        }

        // 合并缓冲区
        override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) = {
            (b1._1 + b2._1, b1._2 + b2._2)
        }

        // 计算最终结果
        override def finish(buff: (Long, Long)): Long = buff._1 / buff._2

        // 缓冲区的编码操作
        override def bufferEncoder: Encoder[(Long, Long)] = Encoders.product

        // 输出的编码操作
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }

}
