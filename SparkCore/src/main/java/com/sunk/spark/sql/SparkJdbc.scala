package com.sunk.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object SparkJdbc {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("SQL")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        /*
         * 读取方式一
         */
        val df1 = spark.read
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://hadoop101:3306/gmall_config")
                .option("user", "root")
                .option("password", "000000")
                .option("dbtable", "table_process")
                .load()

        df1.printSchema()
        df1.show(10, false)


        /*
         * 读取方式二
         */
        val df2 = spark.read
                .format("jdbc")
                .options(Map("url" -> "jdbc:mysql://hadoop101:3306/gmall_config?user=root&password=000000",
                    "dbtable" -> "table_process",
                    "driver" -> "com.mysql.jdbc.Driver")
                ).load()

        /*
         * 读取方式三
         */
        val props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "000000")
        val df3 = spark.read.jdbc("jdbc:mysql://hadoop101:3306/gmall_config", "table_process", props)

        /*
         * 写出方式一
         */
        df1.write
                .format("jdbc")
                .option("url", "jdbc:mysql://hadoop101:3306/gmall_config")
                .option("user", "root")
                .option("password", "000000")
                .option("dbtable", "gmall_config")
                .mode(SaveMode.Append)
                .save()

        /*
         * 写出方式二
         */
        val props2: Properties = new Properties()
        props2.setProperty("user", "root")
        props2.setProperty("password", "000000")
        df1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop101:3306/gmall_config", "gmall_config", props2)


        spark.close()
    }

}
