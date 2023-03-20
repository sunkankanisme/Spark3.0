package com.sunk.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Transform_1value {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark01_RDD_Memory")
        val sc = new SparkContext(sparkConf)


        /*
         * 1 map
         */
        val rdd1 = sc.makeRDD(List(1, 2, 3, 4))

        // 1.1 声明函数
        def mapFunc(num: Int): Int = {
            num * 2
        }

        val mapRdd = rdd1.map(mapFunc)
        mapRdd.collect().foreach(println)

        // 1.2 使用匿名函数
        val mapRdd2 = rdd1.map((num: Int) => {
            num * 2
        })

        // 1.3 进一步简化
        val mapRdd3 = rdd1.map(_ * 2)


        /*
         * 2 mapPartitions
         */

        // 2.1
        val rdd2 = sc.makeRDD(List(1, 2, 3, 4), 2)
        val mapPartitionsRdd = rdd2.mapPartitions(iter => {
            iter.map(_ * 2)
        })

        println(mapPartitionsRdd.collect().mkString("Array(", ", ", ")"))

        // 2.2 小案例，取出分区内的最大值
        val mapPartitionsRdd2 = rdd2.mapPartitions(iter => {
            List(iter.max).iterator
        })
        // Array(2, 4)
        println(mapPartitionsRdd2.collect().mkString("Array(", ", ", ")"))

        /*
         * 3 mapPartitionsWithIndex
         */
        val rdd3 = sc.makeRDD(List(1, 2, 3, 4), 2)

        val mapPartitionsWithIndexRdd = rdd3.mapPartitionsWithIndex((index, iter) => {
            if (index == 1) {
                iter
            } else {
                Nil.iterator
            }
        })

        // Array(3, 4)
        println(mapPartitionsWithIndexRdd.collect().mkString("Array(", ", ", ")"))

        /*
         * 4 flatMap
         */
        // 4.1 相同类型
        val rdd4 = sc.makeRDD(List("Hello World", "Hello Scala"))

        val flatMapRdd = rdd4.flatMap(_.split(" "))

        // Hello,World,Hello,Scala
        println(flatMapRdd.collect().mkString(","))

        // 4.2 不同类型
        val rdd4_2 = sc.makeRDD(List("Hello World", 100, "Hello Scala"))

        // 使用模式匹配
        val flatMapRdd2 = rdd4_2.flatMap(
            data => {
                data match {
                    case str: String => str.split(" ")
                    case num: Int => List(num + "").iterator
                }
            }
        )

        // 简化模式匹配
        val flatMapRdd3 = rdd4_2.flatMap {
            case str: String => str.split(" ")
            case num: Int => List(num + "").iterator
        }

        // flatMapRdd2: Hello,World,100,Hello,Scala
        println("flatMapRdd2: " + flatMapRdd2.collect().mkString(","))

        /*
         * 5 glom
         */

        // 5.1
        val rdd5: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // int => array
        val glomRdd: RDD[Array[Int]] = rdd5.glom()

        // glomRdd: Array([I@58f07f02, [I@75798d03)
        println(s"glomRdd: " + glomRdd.collect().mkString("Array(", ", ", ")"))

        // 5.2 计算所有分区最大值求和(分区内取最大值,分区间最大值求和)
        val glomRdd2 = rdd5.glom()
        val maxRdd = glomRdd2.map(_.max)
        val res = maxRdd.collect().sum
        // 6
        println(res)

        /*
         * 6 groupBy
         */
        // 6.1
        val rdd6 = sc.makeRDD(List(1, 2, 3, 4), 2)

        // 将数据源中的每一个数据进行分组判断，根据返回的分组 key 进行分组
        // 相同的 key 的数据会放置到同一个组中
        def groupFunc(num: Int): Int = {
            num % 2
        }

        // (0,CompactBuffer(2, 4))
        // (1,CompactBuffer(1, 3))
        val groupRdd = rdd6.groupBy(groupFunc)
        groupRdd.collect().foreach(println)

        // 6.2
        // 根据首字母进行分组
        val rdd6_2 = sc.makeRDD(List("Hello", "Spark", "Hello", "Scala"), 2)
        val groupRdd2 = rdd6_2.groupBy(_.charAt(0))
        // (H,CompactBuffer(Hello, Hello)),(S,CompactBuffer(Spark, Scala))
        println(groupRdd2.collect().mkString(","))

        /*
         * 7 filter
         */
        val rdd7 = sc.makeRDD(List("Hello", "Spark", "Hello", "Scala"), 2)
        val filterRdd = rdd7.filter(_.startsWith("H"))
        // Hello,Hello
        println(filterRdd.collect().mkString(","))

        /*
         * 8 sample
         */
        val rdd8 = sc.makeRDD(List(1, 2, 3, 4), 1)
        // 抽取数据不放回(伯努利算法)
        // 伯努利算法: 又叫 0、1 分布。例如扔硬币,要么正面,要么反面。
        // 具体实现: 根据种子和随机算法算出一个数和第二个参数设置几率比较,小于第二个参数要,大于不要
        // 第一个参数: 抽取的数据是否放回,false:不放回
        // 第二个参数: 抽取的几率, 范围在[0,1]之间, 0:全不取; 1:全取;
        // 第三个参数: 随机数种子
        val sampleRdd1 = rdd8.sample(withReplacement = false, 0.5)
        // 抽取数据放回(泊松算法)
        // 第一个参数: 抽取的数据是否放回,true:放回; false:不放回
        // 第二个参数: 重复数据的几率,范围大于等于 0.表示每一个元素被期望抽取到的次数
        // 第三个参数: 随机数种子
        val sampleRdd2 = rdd8.sample(withReplacement = true, 2)

        // 3,4
        println(sampleRdd1.collect().mkString(","))
        // 2,3,3,3,3,3,4
        println(sampleRdd2.collect().mkString(","))

        /*
         * 9 distinct
         */
        val rdd9 = sc.makeRDD(List(1, 2, 3, 2, 3, 4), 2)
        // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
        val distinctRdd = rdd9.distinct()
        // 4,2,1,3
        println(distinctRdd.collect().mkString(","))

        /*
         * 10 coalesce
         */
        val rdd10 = sc.makeRDD(List(1, 2, 3, 2, 3, 4), 2)
        // org.apache.spark.rdd.ParallelCollectionPartition@bda
        // org.apache.spark.rdd.ParallelCollectionPartition@bdb
        rdd10.partitions.foreach(println)
        val coalesceRdd = rdd10.coalesce(1)
        // CoalescedRDDPartition(0,ParallelCollectionRDD[33] at makeRDD at RDD_Operator_Transform.scala:189,[I@abff8b7,None)
        coalesceRdd.partitions.foreach(println)

        rdd10.coalesce(2, true)

        /*
         * 11 repartition
         */
        println("=============== repartition")
        val rdd11 = sc.makeRDD(List(1, 2, 3, 2, 3, 4), 2)
        val repartitionRdd = rdd11.repartition(4)
        repartitionRdd.partitions.foreach(println)

        /*
         * 12 sortBy
         */
        // 12.1
        val rdd12 = sc.makeRDD(List(1, 7, 4, 2, 3, 4), 2)
        val sortByRdd = rdd12.sortBy(num => num)

        // 12.2
        val rdd11_2 = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)))
        val sortByRdd2 = rdd11_2.sortBy(t => t._1, ascending = false)

        sc.stop()
    }

}
