package com.sunk.spark.core.test.distributetask

class Task extends Serializable {

    private val datas: List[Int] = List(1, 2, 3, 4)

    private val logic: Int => Int = (num: Int) => {
        num * 2
    }

    /*
     * 计算
     */
    def compute(): List[Int] = {
        datas.map(logic)
    }

}
