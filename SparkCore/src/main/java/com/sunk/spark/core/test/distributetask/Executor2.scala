package com.sunk.spark.core.test.distributetask

import java.io.ObjectInputStream
import java.net.ServerSocket

/*
 * 测试分布式计算 Driver
 */
object Executor2 {

    def main(args: Array[String]): Unit = {
        // 1 启动服务器接收数据
        val serverSocket = new ServerSocket(9999)

        // 2 等待客户端的连接
        println("9999 服务器启动，等待客户端连接")
        val client = serverSocket.accept()

        // 3 获取客户端的消息
        val inputStream = client.getInputStream
        val objectInputStream = new ObjectInputStream(inputStream)
        val task: SubTask = objectInputStream.readObject().asInstanceOf[SubTask]

        // 执行计算
        val list: List[Int] = task.compute()
        println("9999 接收到 Driver 的计算任务，计算结果为：" + list)

        // 4 关闭资源
        objectInputStream.close()
        inputStream.close()
        client.close()
        serverSocket.close()
    }

}
