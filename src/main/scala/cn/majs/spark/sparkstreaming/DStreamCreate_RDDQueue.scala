package cn.majs.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @program spark-atguigu 
 * @description: 通过RDD队列的方式创建DStream
 * @author: mac 
 * @create: 2020/10/14 21:05 
 */
object DStreamCreate_RDDQueue {
  def main(args: Array[String]): Unit = {
    // 创建配置信息对象
    val conf: SparkConf = new SparkConf().setAppName("DStreamCreate_RDDQueue").setMaster("local[*]")

    // 创建SparkStreaming程序入口
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 创建RDD队列
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    // 读取RDD队列中采集数据，获取DStream                            // oneAtTime=true:每个采集周期只采集一个RDD （默认）
    val queueDS: InputDStream[Int] = ssc.queueStream(rddQueue)  // oneAtTime=false:每个采集周期可以采集多个RDD

    // 处理数据
    val resDS: DStream[(Int, Int)] = queueDS.map((_, 1)).reduceByKey(_ + _)

    // 打印结果
    resDS.print()

    // 启动采集器
    ssc.start()

    // 循环创建RDD 并将创建的RDD放在队列中
    for(i <- 1 to 5){
      rddQueue.enqueue(ssc.sparkContext.makeRDD(6 to 10))
      Thread.sleep(2000)
    }

    // 采集完关闭
    ssc.awaitTermination()

  }
}
