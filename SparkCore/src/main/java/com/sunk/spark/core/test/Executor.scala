package com.sunk.spark.core.test

import java.net.ServerSocket

/*
 * 测试分布式计算 Driver
 */
object Executor {

    def main(args: Array[String]): Unit = {
        // 1 启动服务器接收数据
        val serverSocket = new ServerSocket(9999)

        // 2 等待客户端的连接
        println("服务器启动，等待客户端连接")
        val client = serverSocket.accept()

        // 3 获取客户端的消息
        val inputStream = client.getInputStream
        val i = inputStream.read()
        println("接收到客户端发送的数据：" + i)

        // 4 关闭资源
        inputStream.close()
        client.close()
        serverSocket.close()
    }

}
