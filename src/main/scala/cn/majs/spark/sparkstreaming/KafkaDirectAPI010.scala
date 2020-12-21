package cn.majs.spark.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @program spark-atguigu 
 * @description: sparkStreaming通过 DirectAPI（0.10）消费Kafka
 * @author: mac 
 * @create: 2020/10/23 10:23 
 */
object KafkaDirectAPI010 {
  def main(args: Array[String]): Unit = {
    // 创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaDirectAPI010")

    // 创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    // 构建kafka连接相关对象
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node01:9092,node02:9092,node03:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //通过Kafka读取数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      // 位置策略指定计算的Excutor
      LocationStrategies.PreferConsistent,
      // 消费策略
      ConsumerStrategies.Subscribe[String, String](Set("bigdata0105"), kafkaParams)
    )

    // 处理逻辑
    kafkaDStream.map(_.value())
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).print()

    // 启动
    ssc.start()

    // 消费完关闭
    ssc.awaitTermination()
  }
}
