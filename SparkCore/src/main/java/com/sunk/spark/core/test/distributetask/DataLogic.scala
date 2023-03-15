package com.sunk.spark.core.test.distributetask

class DataLogic extends Serializable {

    val datas: List[Int] = List(1, 2, 3, 4)

    val logic: Int => Int = (num: Int) => {
        num * 2
    }

}
