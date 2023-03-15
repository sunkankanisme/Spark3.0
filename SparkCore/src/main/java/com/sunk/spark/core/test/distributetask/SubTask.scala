package com.sunk.spark.core.test.distributetask

/**
 * 子任务
 */
class SubTask extends Serializable {

    var data: List[Int] = _

    var logic: Int => Int = _

    /*
     * 计算
     */
    def compute(): List[Int] = {
        data.map(logic)
    }

}
