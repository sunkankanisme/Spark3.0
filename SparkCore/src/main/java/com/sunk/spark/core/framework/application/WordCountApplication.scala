package com.sunk.spark.core.framework.application

import com.sunk.spark.core.framework.common.TApplication
import com.sunk.spark.core.framework.controller.WordCountController

// 应用程序启动类
object WordCountApplication extends App with TApplication {
    start(appName = "WordCountApplication") {
        val controller = new WordCountController()
        controller.dispatch()
    }
}
