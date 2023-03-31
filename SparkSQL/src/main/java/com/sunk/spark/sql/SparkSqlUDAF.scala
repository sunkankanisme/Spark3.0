package com.sunk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object SparkSqlUDAF {

    def main(args: Array[String]): Unit = {
        // 创建环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("SQL")
        // .enableHiveSupport()
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val df = spark.read.json("SparkCore/src/main/resources/data/test.json")

        df.createOrReplaceTempView("user")

        // 使用 udaf 计算平均值
        spark.udf.register("avg_age", new MyUDAF)

        spark.sql("select avg_age(age) from user").show

        spark.close()
    }

    /*
     * 计算年龄的平均值
     * 1 继承 UserDefinedAggregateFunction 类
     * 2 重复方法
     */
    class MyUDAF extends UserDefinedAggregateFunction {
        // 定义输入结构 - IN
        override def inputSchema: StructType = {
            StructType(
                Array(
                    StructField("age", LongType)
                )
            )
        }

        // 定义缓冲区结构 - MIDDLE
        override def bufferSchema: StructType = {
            StructType(
                Array(
                    StructField("total", LongType),
                    StructField("count", LongType)
                )
            )
        }

        // 函数计算结果的类型 - OUT
        override def dataType: DataType = LongType

        // 函数的稳定性 - 传入相同的参数输出是否相同
        override def deterministic: Boolean = true

        // 中间缓冲区初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = 0L
        }

        // 输入一条数据之后如何更新缓冲区
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            // 使用输入数据（与之前定义的格式相同）的第一个字段的值，与缓冲器的值进行累加
            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) + 1)
        }

        // 合并操作（分布式计算）
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        // 使用缓冲区的数据计算结果
        override def evaluate(buffer: Row): Any = buffer.getLong(0) / buffer.getLong(1)
    }

}
