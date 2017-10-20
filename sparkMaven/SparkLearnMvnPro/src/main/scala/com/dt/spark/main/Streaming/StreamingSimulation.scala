package com.dt.spark.main.Streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

/**
  * Created by hjw on 17/5/1.
  */
object StreamingSimulation {
  /*
  随机取整函数
   */
  def index(length:Int) ={
    import java.util.Random
    val rdm = new Random()
    rdm.nextInt(length)
  }

  def main(args: Array[String]) {
    if (args.length != 3){
      System.err.println("Usage: <filename><port><millisecond>")
      System.exit(1)
    }

    val filename = args(0)
    val lines = Source.fromFile(filename).getLines().toList
    val fileRow = lines.length

    val listener = new ServerSocket(args(1).toInt)

    //指定端口,当有请求时建立连接
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run() = {
          println("Got client connect from: " + socket.getInetAddress)
          val out =  new PrintWriter(socket.getOutputStream,true)
          while(true){
            Thread.sleep(args(2).toLong)
            //随机发送一行数据至client
            val content = lines(index(fileRow))
            println("-------------------------------------------")
            println(s"Time: ${System.currentTimeMillis()} ms")
            println("-------------------------------------------")
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
