package com.sunk.spark.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerializableTest {
    def main(args: Array[String]): Unit = {
        //1.创建 SparkConf 并设置 App 名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建 SparkContext,该对象是提交 Spark App 的入口
        val sc: SparkContext = new SparkContext(conf)

        //3.创建一个 RDD
        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        //3.1 创建一个 Search 对象
        val search = new Search("hello")

        //3.2 函数传递,打印:ERROR Task not serializable
        search.getMatch1(rdd).collect().foreach(println)

        //3.3 属性传递,打印:ERROR Task not serializable
        search.getMatch2(rdd).collect().foreach(println)

        //4.关闭连接
        sc.stop()
    }

    /*
     * scala 中类的构造参数是类的属性
     * 构造参数需要进行闭包检测，其实就等同于需要对类进行闭包检测
     */
    private class Search(query: String) extends Serializable {

        def isMatch(s: String): Boolean = {
            s.contains(this.query)
            s.contains(query)
        }

        // 函数序列化案例
        def getMatch1(rdd: RDD[String]): RDD[String] = {
            // rdd.filter(this.isMatch)
            rdd.filter(isMatch)
        }

        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            //rdd.filter(x => x.contains(this.query))
            rdd.filter(x => x.contains(query))

            // 使用局部变量的方法解决
            // val q = query
            // rdd.filter(x => x.contains(q))
        }
    }
}
