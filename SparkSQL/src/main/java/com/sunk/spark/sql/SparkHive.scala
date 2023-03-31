package com.sunk.spark.sql

import org.apache.spark.sql.SparkSession

object SparkHive {

    def main(args: Array[String]): Unit = {
        val session = SparkSession.builder()
                .appName("SparkSQL")
                .master("local[]")
                .enableHiveSupport()
                .getOrCreate()

        session.sql("show databases").show()

        session.close()
    }

}
