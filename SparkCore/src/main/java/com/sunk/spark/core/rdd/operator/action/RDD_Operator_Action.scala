package com.sunk.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("TransformDemo")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        /*
         * 1 reduce
         * - 转换算子的结果是生成 RDD，而行动算子会直接生成结果
         */
        val res1: Int = rdd.reduce(_ + _)
        // 10
        println(res1)

        /*
         * 2 collect
         */
        val res2 = rdd.collect()
        // Array(1, 2, 3, 4)
        println(res2.mkString("Array(", ", ", ")"))

        /*
         * 3 count
         */
        val res3 = rdd.count()
        // 4
        println(res3)

        /*
         * 4 first
         */
        val res4 = rdd.first()
        // 1
        println(res4)

        /*
         * 5 take
         */
        val res5: Array[Int] = rdd.take(2)
        // Array(1, 2)
        println(res5.mkString("Array(", ", ", ")"))

        /*
         * 6 takeOrdered
         */
        val rdd2 = sc.makeRDD(List(4, 2, 3, 1))
        val res6 = rdd2.takeOrdered(2)
        // Array(1, 2)
        println(res6.mkString("Array(", ", ", ")"))

        val res6_2 = rdd2.takeOrdered(2)(Ordering.Int.reverse)
        // Array(4, 3)
        println(res6_2.mkString("Array(", ", ", ")"))

        /*
         * 7 aggregate
         */
        // (初始值)(分区内计算逻辑, 分区间计算逻辑)
        val res7: Int = rdd.aggregate(0)(_ + _, _ + _)
        // 10
        println(res7)

        /*
         * 8 fold
         */
        val res8: Int = rdd.fold(0)(_ + _)
        // 10
        println(res8)

        /*
         * 9 countByKey
         */
        val rdd3 = sc.makeRDD(List(1, 2, 1, 3, 4))
        val res9_1 = rdd3.countByValue()
        // 4 -> 1,1 -> 2,3 -> 1,2 -> 1
        println(res9_1.mkString(","))

        val rdd4 = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 1)))
        val res9_2 = rdd4.countByKey()
        // a -> 2,b -> 1
        println(res9_2.mkString(","))

        /*
         * 10 save
         */
        rdd.saveAsTextFile("SparkCore/src/main/resources/output/text_file")
        rdd.saveAsObjectFile("SparkCore/src/main/resources/output/obj_file")
        rdd.map((_, 1)).saveAsSequenceFile("SparkCore/src/main/resources/output/text_file")

        /*
         * 11 foreach
         */
        rdd.foreach(println)
        rdd.foreachPartition(iter => iter.foreach(println))


        sc.stop()
    }

}
