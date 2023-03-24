package com.sunk.spark.core.framework.service

import com.sunk.spark.core.framework.common.TService
import com.sunk.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * wc 服务层
 */
class WordCountService extends TService {

    private val wordCountDao = new WordCountDao()

    // 在服务层进行数据分析
    def dataAnalysis(): Array[(String, Int)] = {

        val fileRdd = wordCountDao.readFile("SparkCore/src/main/resources/input/word.txt")

        val wordRdd: RDD[String] = fileRdd.flatMap(line => line.split(" "))

        val word2CountRdd: RDD[(String, Int)] = wordRdd.map(word => (word, 1))

        word2CountRdd.reduceByKey(_ + _).collect()
    }

}
