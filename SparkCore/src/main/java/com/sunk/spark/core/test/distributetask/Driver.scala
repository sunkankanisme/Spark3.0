package com.sunk.spark.core.test.distributetask

import java.io.ObjectOutputStream
import java.net.Socket

/*
 * 测试分布式计算 Driver
 */
object Driver {

    def main(args: Array[String]): Unit = {
        // 1 连接服务器
        val client = new Socket("localhost", 9999)

        // 2 向 Executor 发送任务
        val outputStream = client.getOutputStream
        val objectOutputStream = new ObjectOutputStream(outputStream)

        // 封装任务
        val task = new Task()
        objectOutputStream.writeObject(task)
        objectOutputStream.close()
        outputStream.close()
        println("向 Executor 发送任务完成")

        // 3 关闭资源
        outputStream.close()
        client.close()
    }

}
