package com.sunk.spark.core.framework.controller

import com.sunk.spark.core.framework.common.TController
import com.sunk.spark.core.framework.service.WordCountService

/**
 * wc 控制层
 */
class WordCountController extends TController {

    private val wordCountService = new WordCountService()

    // 任务调度
    def dispatch(): Unit = {
        val array: Array[(String, Int)] = wordCountService.dataAnalysis()
        println(array.mkString("Array(", ", ", ")"))
    }


}
