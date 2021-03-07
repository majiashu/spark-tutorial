package cn.majs.spark.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @program spark-tutorial 
 * @description: ${TODO} 
 * @author: mac 
 * @create: 2021/03/06 17:46 
 */
object UpdateStateByKeyTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 设置配置信息  注意此处是local[*]
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingWordCount")
      .setMaster("local[*]")  //  在集群上运行的时候就不要设置
      .setAppName("UpdateStateByKey")

    // 创建sparkStreaming程序入口
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 设置一个checkpoint目录用以保存历史状态值，一般设置HDFS上的路径
    ssc.checkpoint("/Users/mac/IdeaProjects/spark-tutorial/input")

    // 从数据源（端口9999）中获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("node04", 9999)

    // 扁平化，按对应的分隔符切分
    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))

    // 结构转换
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_, 1))

    // UpdateStateByKey保存历史状态，需要设置checkporint
    val resDS: DStream[(String, Int)] = mapDS.updateStateByKey((value: Seq[Int], state: Option[Int]) => {
      val currCount = value.sum
      val lastCount = state.getOrElse(0)
      // 在scala中最后一行就是返回值
      Some(currCount + lastCount)
    })

    // 对数据进行聚合
    //val resDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)

    //打印
    resDS.print()

    // 启动采集器
    ssc.start()

    //等待采集结束，终止上下文环境对象
    ssc.awaitTermination()

    // 默认情况下采集器不能关闭
    ssc.stop()
  }
}
