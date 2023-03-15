package com.sunk.spark.core.test.distributetask

import java.io.ObjectOutputStream
import java.net.Socket

/*
 * 测试分布式计算 Driver
 */
object Driver {

    def main(args: Array[String]): Unit = {
        // 1 连接服务器
        val client1 = new Socket("localhost", 8899)
        val client2 = new Socket("localhost", 9999)

        // 2 封装任务
        val task = new DataLogic()
        val subTask1 = new SubTask()
        subTask1.logic = task.logic
        subTask1.data = task.datas.take(2)

        // 3 向 Executor1 发送任务
        val outputStream1 = client1.getOutputStream
        val objectOutputStream1 = new ObjectOutputStream(outputStream1)
        objectOutputStream1.writeObject(subTask1)
        objectOutputStream1.close()
        outputStream1.close()
        println("向 Executor 1 发送任务完成")

        // 3 关闭资源
        outputStream1.close()
        client1.close()

        // 4 封装任务
        val subTask2 = new SubTask()
        subTask2.logic = task.logic
        subTask2.data = task.datas.takeRight(2)

        // 3 向 Executor2 发送任务
        val outputStream2 = client2.getOutputStream
        val objectOutputStream2 = new ObjectOutputStream(outputStream2)
        objectOutputStream2.writeObject(subTask2)
        objectOutputStream2.close()
        outputStream2.close()
        println("向 Executor 2 发送任务完成")

        // 3 关闭资源
        outputStream2.close()
        client2.close()
    }

}
