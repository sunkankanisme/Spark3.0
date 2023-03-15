package com.sunk.spark.core.test

import java.net.Socket

/*
 * 测试分布式计算 Driver
 */
object Driver {

    def main(args: Array[String]): Unit = {
        // 1 连接服务器
        val client = new Socket("localhost", 9999)

        // 2 向 Executor
        val outputStream = client.getOutputStream
        outputStream.write(2)
        outputStream.flush()

        // 3 关闭资源
        outputStream.close()
        client.close()
    }

}
