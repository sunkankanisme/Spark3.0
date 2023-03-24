package com.sunk.spark.core.framework.common

import com.sunk.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

trait TDao {

    def readFile(path: String): RDD[String] = {
        EnvUtil.take().textFile(path)
    }

}
