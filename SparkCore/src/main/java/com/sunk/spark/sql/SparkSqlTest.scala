package com.sunk.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SparkSqlTest {

    def main(args: Array[String]): Unit = {
        // 创建环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("SQL")
        // .enableHiveSupport()
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        /*
         * 1 DataFrame
         */
        val df: DataFrame = spark.read.json("SparkCore/src/main/resources/data/test.json")
        df.show()

        // 1.1 DataFrame SQL
        println("===== DataFrame SQL")
        df.createOrReplaceTempView("view_df")
        val frame: DataFrame = spark.sql("select * from view_df where age > 100")
        frame.show()

        // 1.2 DataFrame DSL
        println("===== DataFrame DSL")
        df.select('age, 'username).filter('age > 100).show()
        df.select($"age", $"username").filter($"age" > 100).show()
        
        /*
         * 2 DataSet
         */
        val ints = Seq(1, 2, 3, 4)
        val ds: Dataset[Int] = ints.toDS()
        ds.createOrReplaceTempView("view_ds")
        spark.sql("select * from view_ds").show()


        /*
         * 3 RDD <=> DataFrame
         */
        // 3.1 rdd -> df
        val rdd = spark.sparkContext.makeRDD(List(
            (1, "zhangsan", 30),
            (2, "lisi", 40)
        ))

        val df1 = rdd.toDF("id", "name", "age")

        // 3.2 df -> rdd
        val rdd1: RDD[Row] = df.rdd

        /*
         * 4 DataSet <=> DataFrame
         */
        // 4.1 DataFrame -> DataSet
        val ds1: Dataset[User] = df1.as[User]
        // 4.2 DataSet -> DataFrame
        val df2: DataFrame = ds1.toDF()

        /*
         * 5 RDD <=> DataSet
         */
        // 5.1 RDD -> DS
        val ds2: Dataset[User] = rdd.map {
            case (id, name, age) =>
                User(id, name, age)
        }.toDS()

        // 5.2 DS -> RDD
        val rdd2: RDD[User] = ds2.rdd

        // 关闭环境
        spark.close()
    }

    case class User(id: Int, name: String, age: Int)

}
