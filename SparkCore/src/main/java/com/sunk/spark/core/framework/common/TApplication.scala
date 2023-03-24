package com.sunk.spark.core.framework.common

import com.sunk.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

    def start(master: String = "local[*]", appName: String = "Application")(op: => Unit): Unit = {
        // 任务环境在 app 层定义
        val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
        val sc = new SparkContext(sparkConf)
        EnvUtil.put(sc)

        try {
            op
        } catch {
            case e: Exception =>
                e.printStackTrace()
        }

        sc.stop()
        EnvUtil.clear()
    }

}
