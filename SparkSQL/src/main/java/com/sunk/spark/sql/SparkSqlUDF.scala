package com.sunk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlUDF {

    def main(args: Array[String]): Unit = {
        // 创建环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("SQL")
        // .enableHiveSupport()
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val df = spark.read.json("SparkCore/src/main/resources/data/test.json")

        df.createOrReplaceTempView("user")

        // 使用 udf 完成给名字加前缀的功能
        // spark.sql("select age, 'NAME-' || username from user").show
        spark.udf.register("prefix_name", (name: String) => {
            "NAME-" + name
        })

        spark.sql("select age, prefix_name(username) from user").show

        spark.close()
    }

}
