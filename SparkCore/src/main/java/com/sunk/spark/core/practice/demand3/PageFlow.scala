package com.sunk.spark.core.practice.demand3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * 页面单跳转换率
 */
object PageFlow {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
        val sc: SparkContext = new SparkContext(conf)

        // 1 读取原始日志
        val dataRdd: RDD[String] = sc.textFile("SparkCore/src/main/resources/data/user_visit_action.txt")

        val actionDataRdd = dataRdd.map(data => {
            val strings = data.split("_")
            UserVisitAction(
                strings(0),
                strings(1).toLong,
                strings(2),
                strings(3).toLong,
                strings(4),
                strings(5),
                strings(6).toLong,
                strings(7).toLong,
                strings(8),
                strings(9),
                strings(10),
                strings(11),
                strings(12).toLong
            )
        })

        actionDataRdd.cache()

        // TODO 对指定的连续页面跳转进行统计
        val ids = List(1, 2, 3, 4, 5, 6, 7)
        val zips = ids.zip(ids.tail)


        // 1 计算分母

        val pageIdToCount: Map[Long, Int] = actionDataRdd.filter(action => {
            ids.contains(action.page_id)
        })
                .map(action => (action.page_id, 1))
                .reduceByKey(_ + _)
                .collect()
                .toMap

        // 2 计算分子
        val sessionRdd: RDD[(String, Iterable[UserVisitAction])] = actionDataRdd.groupBy(_.session_id)

        val mapRdd: RDD[(String, List[((Long, Long), Int)])] = sessionRdd.mapValues(itr => {
            val visitActions = itr.toList.sortBy(_.action_time)
            val flowIds = visitActions.map(_.page_id)
            // 创建连接关系
            // [1,2,3,4] => [1-2, 2-3, 3-4]
            val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

            // 将不相关的跳转过滤掉
            pageFlowIds.filter(zips.contains(_)).map(t => (t, 1))
        })

        // ((1,2), 1)
        val flatRdd: RDD[((Long, Long), Int)] = mapRdd.map(_._2).flatMap(list => list)
        // ((1,2), sum)
        val fenzi = flatRdd.reduceByKey(_ + _)

        // 3 计算比率
        fenzi.foreach {
            case ((pageId1, pageId2), sum) =>
                val lon = pageIdToCount.getOrElse(pageId1, 0)
                println(s"页面单跳转换率 [$pageId1 -> $pageId2]: ${sum.toDouble / lon}")
        }


        sc.stop()
    }

    case class UserVisitAction(date: String, //用户点击行为的日期
                               user_id: Long, //用户的 ID
                               session_id: String, //Session 的 ID
                               page_id: Long, //某个页面的 ID
                               action_time: String, //动作的时间点
                               search_keyword: String, //用户搜索的关键词
                               click_category_id: Long, //某一个商品品类的 ID
                               click_product_id: Long, //某一个商品的 ID
                               order_category_ids: String, //一次订单中所有品类的 ID 集合
                               order_product_ids: String, //一次订单中所有商品的 ID 集合
                               pay_category_ids: String, //一次支付中所有品类的 ID 集合
                               pay_product_ids: String, //一次支付中所有商品的 ID 集合
                               city_id: Long
                              )
}
