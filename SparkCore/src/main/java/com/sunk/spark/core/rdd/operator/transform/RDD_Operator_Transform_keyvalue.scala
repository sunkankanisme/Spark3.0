package com.sunk.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDD_Operator_Transform_keyvalue {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark01_RDD_Memory")
        val sc = new SparkContext(sparkConf)


        /*
         * 1 partitionBy
         */
        val rdd1: RDD[(Int, Int)] = sc.makeRDD(List(1, 2, 3, 4), 2).map((_, 1))

        // RDD => PairRDDFunctions（隐式转换，二次编译）
        // 根据指定的规则对数据进行重分区，区别于 repartition 算子（只是修改了分区数，并没有规律）
        val partitionByRdd = rdd1.partitionBy(new HashPartitioner(2))
        // (2,1),(4,1)
        // (1,1),(3,1)
        // partitionByRdd.saveAsTextFile("SparkCore\\src\\main\\resources\\output\\partitionByRdd")

        /*
         * 2 reduceByKey
         */
        val rdd2 = sc.makeRDD(List(
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("b", 4)
        ))

        // spark 是基于 scala 开发的，所以聚合的方式也是两两聚合
        // a: [1,2,3] => [3,3] => [6]
        // b: [4]
        val reduceByKeyRdd = rdd2.reduceByKey((x, y) => x + y)
        // (a,6),(b,4)
        println(reduceByKeyRdd.collect().mkString(","))

        /*
         * 3 groupByKey
         */
        val rdd3 = sc.makeRDD(List(
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("b", 4)
        ))

        // groupByKey 会将数据源中相同 key 的数据分到一个组中，形成一个对偶元组
        // 元组中的第一个元素就是 key，第二个元素就是相同 key 的 value 集合
        // a: [1, 2, 3]
        // b: [4]
        val groupByKeyRdd: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
        // (a,CompactBuffer(1, 2, 3)),(b,CompactBuffer(4))
        println(groupByKeyRdd.collect().mkString(","))

        // 区别于 groupBy
        val groupByRdd: RDD[(String, Iterable[(String, Int)])] = rdd3.groupBy(_._1)

        /*
         * 4 aggregateByKey
         */

        // 要求分区内相同 key 取最大值，分区间求和
        // (a, [1,2]), (a, [3,4]) => a[2], a[4] => a[6]
        val rdd4 = sc.makeRDD(List(
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4)
        ), 2)

        // aggregateByKey 存在函数的科里化，有两个参数列表
        // 第一个参数列表传递一个参数，表示初始值
        // - 主要用于当碰见第一个 key 的时候（使用初始值）和 value 进行分区内计算
        // 第二个参数列表传递两个方法参数：
        // - 第一个参数表示分区内计算规则
        // - 第二个参数表示分区间计算规则
        val aggregateByKeyRdd = rdd4.aggregateByKey(0)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )

        // (a,6)
        println(aggregateByKeyRdd.collect().mkString(","))

        // 使用 aggregate 计算每个 key 的平均值
        // 参数一：元组 (出现次数，总和)
        // 返回值：类型为（key，初始值类型）
        val tmpRdd: RDD[(String, (Int, Int))] = rdd4.aggregateByKey((0, 0))(
            (t, v) => {
                // 分区内：次数累加，总和相加
                (t._1 + 1, t._2 + v)
            },
            (t1, t2) => {
                // 分区间：次数累加，总和相加
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )

        // 对于 kv 类型的数据，key 保持不变只对 value 进行操作
        val resultRdd: RDD[(String, Double)] = tmpRdd.mapValues {
            case (cnt, num) => num.toDouble / cnt
        }

        // === (a,2)
        println("=== " + resultRdd.collect().mkString(","))



        /*
         * 5 foldByKey
         */

        val rdd5 = sc.makeRDD(List(
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4)
        ), 2)
        // 分区内和分区间都取最大值
        val foldByKeyRdd = rdd5.foldByKey(0)(math.max)
        // (a,4)
        println(foldByKeyRdd.collect().mkString(","))


        /*
         * 6 foldByKey
         */

        val rdd6 = sc.makeRDD(List(
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4)
        ), 2)

        /*
         * 三个参数：
         * - 参数一：相同 key 的第一个数据进行转换的操作
         * - 参数二：分区内的计算规则
         * - 参数三：分区间的计算柜内
         */
        val combineByKeyRdd: RDD[(String, (Int, Int))] = rdd6.combineByKey(
            v => (v, 1),
            (t: (Int, Int), v) => {
                (t._1 + v, t._2 + 1)
            },
            (t1: (Int, Int), t2: (Int, Int)) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )

        // combineByKeyRdd: (a,(10,4))
        println("combineByKeyRdd: " + combineByKeyRdd.collect().mkString(","))

        /*
         * 7 join
         */
        val rdd7_1 = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 10)
        ))

        val rdd7_2 = sc.makeRDD(List(
            ("a", 4),
            ("b", 5),
            ("c", 6)
        ))

        // 要求 key 类型相同
        // 匹配不到的 key 不会输出
        // 重复的 key 会产生笛卡尔积
        val joinRdd = rdd7_1.join(rdd7_2)
        // join: (a,(1,4)),(b,(2,5)),(c,(3,6))
        println("join: " + joinRdd.collect().mkString(","))


        /*
         * 8 leftOuterJoin
         */
        val rdd8_1 = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 10)
        ))

        val rdd8_2 = sc.makeRDD(List(
            ("a", 4),
            ("b", 5),
            ("c", 6)
        ))

        // 找不到为 None
        val leftOuterJoinRdd = rdd8_1.leftOuterJoin(rdd8_2)
        // (d,(10,None)),(a,(1,Some(4))),(b,(2,Some(5))),(c,(3,Some(6)))
        println(leftOuterJoinRdd.collect().mkString(","))


        /*
         * 9 cogroup
         * - connect + group
         * - 相同的 key 放在一个组中再进行连接
         */
        val rdd9_1 = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 10)
        ))

        val rdd9_2 = sc.makeRDD(List(
            ("a", 4),
            ("b", 5),
            ("c", 6),
            ("c", 600),
        ))

        val cogroupRdd = rdd9_1.cogroup(rdd9_2)
        // cogroup:
        // (d,(CompactBuffer(10),CompactBuffer())),
        // (a,(CompactBuffer(1),CompactBuffer(4))),
        // (b,(CompactBuffer(2),CompactBuffer(5))),
        // (c,(CompactBuffer(3),CompactBuffer(6, 600)))
        println("cogroup: " + cogroupRdd.collect().mkString(","))


        /*
         * 10 sortByKey
         */
        val rdd10 = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 10)
        ))

        val sortByKeyRdd1 = rdd10.sortByKey(ascending = true)
        // (a,1),(b,2),(c,3),(d,10)
        println(sortByKeyRdd1.collect().mkString(","))
        val sortByKeyRdd2 = rdd10.sortByKey(ascending = false)
        // (d,10),(c,3),(b,2),(a,1)
        println(sortByKeyRdd2.collect().mkString(","))


        sc.stop()
    }

}
