package cn.majs.spark.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @program spark-atguigu 
 * @description: sparkStreaming WordCount案例
 * @author: mac 
 * @create: 2020/10/13 17:53 
 */
object SparkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    // 设置配置信息  注意此处是local[*]
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[*]")

    // 创建sparkStreaming程序入口
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 从数据源（端口9999）中获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("node04", 9999)

    // 扁平化，按对应的分隔符切分
    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))

    // 结构转换
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_, 1))

    // 对数据进行聚合
    val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)

    //打印
    resDS.print()

    // 启动采集器
    ssc.start()

    // 默认情况下采集器不能关闭
    // ssc.stop()

    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()
  }

}
