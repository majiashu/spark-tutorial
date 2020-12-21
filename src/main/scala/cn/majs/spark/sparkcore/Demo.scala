package cn.majs.spark.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-tutorial 
 * @description: 演示
 * @author: mac 
 * @create: 2020/12/21 16:30 
 */
object Demo {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Demo").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 读取数据源
    val textRDD: RDD[String] = sc.textFile("input/input.txt")

    val mapRDD: RDD[Array[String]] = textRDD.map(line => line.split("\\s"))
    mapRDD.collect()

    val flatMapRDD: RDD[String] = textRDD.flatMap(line => line.split("\\s"))
    flatMapRDD.collect()


    // 关闭连接
    sc.stop()


  }

}
