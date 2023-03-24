package com.sunk.spark.core.practice.demand1

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object HotCategory4 {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        // 1 读取原始日志
        val dataRdd: RDD[String] = sc.textFile("SparkCore/src/main/resources/data/user_visit_action.txt")

        // 2 将数据转换结构
        // TODO 使用累加器解决 shuffle 问题
        val categoryAcc = new HotCategoryAcc()
        sc.register(categoryAcc, "categoryAcc")

        dataRdd.foreach(line => {
            val strings = line.split("_")

            if (strings(6) != "-1") {
                // 点击
                categoryAcc.add(strings(6), "click")
            } else if (strings(8) != "null") {
                // 下单
                strings(8).split(",").foreach(id => categoryAcc.add(id, "order"))
            } else if (strings(10) != "null") {
                // 支付
                strings(10).split(",").foreach(id => categoryAcc.add(id, "pay"))
            }
        })

        // 排序打印输出
        val result: Iterable[HotCategory] = categoryAcc.value.values

        // 排序，需要考虑相等的情况
        result.toList.map(h => (h.cid, (h.clickCnt, h.orderCnt, h.payCnt))).sortBy(_._2).reverse.take(10).foreach(println)

        dataRdd.unpersist()
        sc.stop()
    }

    /*
     * 自定义累加器
     * IN：(ID, 类型)
     * OUT：mutable.Map[String, HotCategory]
     */
    class HotCategoryAcc extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
        private val map = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = map.isEmpty

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAcc

        override def reset(): Unit = map.clear()

        /*
         * 添加数据
         */
        override def add(v: (String, String)): Unit = {
            val cid = v._1
            val actionType = v._2
            val category: HotCategory = map.getOrElse(cid, HotCategory(cid, 0, 0, 0))

            if (actionType == "click") {
                category.clickCnt += 1
            } else if (actionType == "order") {
                category.orderCnt += 1
            } else if (actionType == "pay") {
                category.payCnt += 1
            }

            map.update(cid, category)
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            other.value.foreach(m => {
                val cid: String = m._1
                val otherCategory: HotCategory = m._2

                val category = map.getOrElse(cid, HotCategory(cid, 0, 0, 0))
                category.clickCnt += otherCategory.clickCnt
                category.orderCnt += otherCategory.orderCnt
                category.payCnt += otherCategory.payCnt

                map.update(cid, category)
            })
        }

        override def value: mutable.Map[String, HotCategory] = map
    }

    case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

}
