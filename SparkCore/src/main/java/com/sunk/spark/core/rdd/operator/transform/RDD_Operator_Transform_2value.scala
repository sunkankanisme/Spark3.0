package com.sunk.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_2value {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark01_RDD_Memory")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
        val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

        /*
         * 1 intersection - 交集
         */
        val intersectionRdd = rdd1.intersection(rdd2)
        // 4,3
        println(intersectionRdd.collect().mkString(","))

        /*
         * 2 union - 并集
         */
        val unionRdd = rdd1.union(rdd2)
        // 1,2,3,4,3,4,5,6
        println(unionRdd.collect().mkString(","))

        /*
         * 3 subtract - 差集
         */
        val subtractRdd = rdd1.subtract(rdd2)
        // 1,2
        println(subtractRdd.collect().mkString(","))
        val subtractRdd2 = rdd2.subtract(rdd1)
        // 5,6
        println(subtractRdd2.collect().mkString(","))

        /*
         * 4 zip - 拉链
         */
        val zipRdd = rdd1.zip(rdd2)
        // (1,3),(2,4),(3,5),(4,6)
        println(zipRdd.collect().mkString(","))


        sc.stop()
    }

}
