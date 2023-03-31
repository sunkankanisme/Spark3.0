package com.sunk.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkPractice {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")
        val session = SparkSession.builder()
                .appName("SparkSQL")
                .master("local[1]")
                .enableHiveSupport()
                .getOrCreate()


        // 查询基本数据
        session.sql(
            """
              | select a.*,
              |        p.product_name,
              |        c.area,
              |        c.city_name
              | from spark_sql.user_visit_action a
              |          join spark_sql.product_info p on a.click_product_id = p.product_id
              |          join spark_sql.city_info c on a.city_id = c.city_id
              | where a.click_product_id > -1
              |""".stripMargin).createOrReplaceTempView("t1")

        // 根据区域和商品进行聚合
        session.udf.register("city_remark", functions.udaf(new CityRemark()))
        session.sql(
            """
              | select area,
              |        product_name,
              |        count(*) as click_count,
              |        city_remark(city_name) as city_remark
              | from t1
              | group by area, product_name
              |""".stripMargin).createOrReplaceTempView("t2")

        // 区域内对点击数量进行排行
        session.sql(
            """
              | select *,
              |     rank() over (partition by area order by click_count desc ) as rk
              | from  t2
              |""".stripMargin).createOrReplaceTempView("t3")

        // 取前三名
        session.sql(
            """
              | select *
              | from t3
              | where rk <= 3
              |""".stripMargin).show(false)

        session.close()
    }

    /*
     * 自定义 UDF 完成城市备注功能
     */
    case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

    class CityRemark extends Aggregator[String, Buffer, String] {
        // 初始化缓冲区
        override def zero: Buffer = Buffer(0, mutable.Map[String, Long]())

        // 更新缓冲区
        override def reduce(buffer: Buffer, city: String): Buffer = {
            buffer.total += 1
            val newCount = buffer.cityMap.getOrElse(city, 0L) + 1
            buffer.cityMap.update(city, newCount)
            buffer
        }

        // 合并缓冲区
        override def merge(b1: Buffer, b2: Buffer): Buffer = {
            b1.total += b2.total

            val c1Map = b1.cityMap
            val c2Map = b2.cityMap
            // b1.cityMap = c1Map.foldLeft(c2Map) {
            //     case (map, (city, cnt)) =>
            //         val newCount = map.getOrElse(city, 0L) + cnt
            //         map.update(city, newCount)
            //         map
            // }

            c2Map.foreach {
                case (city, count) =>
                    val newCount = c1Map.getOrElse(city, 0L) + count
                    c1Map.update(city, newCount)
            }
            b1.cityMap = c1Map
            b1
        }

        // 使用 Buffer 中数据生成结果
        override def finish(buffer: Buffer): String = {
            val remarkList = ListBuffer[String]()

            val totalCount = buffer.total
            val cityMap = buffer.cityMap

            // 降序排列
            val cityCntList = cityMap.toList.sortWith((left, right) => {
                left._2 > right._2
            }).take(2)

            var other = 100L
            cityCntList.foreach {
                case (city, count) =>
                    val r: Long = count * 100 / totalCount
                    remarkList.append(s"$city $r%")
                    // 减去已经计算的
                    other -= r
            }

            // 计算其他
            if (cityMap.size > 2) {
                remarkList.append(s"其他 $other%")
            }

            remarkList.mkString(",")
        }

        override def bufferEncoder: Encoder[Buffer] = Encoders.product

        override def outputEncoder: Encoder[String] = Encoders.STRING
    }

}
