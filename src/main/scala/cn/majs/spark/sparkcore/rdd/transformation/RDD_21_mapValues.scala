package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子 mapValues 对于kv类型的RDD只对V进行操作
 * @author: mac 
 * @create: 2020/10/06 18:37 
 */
object RDD_21_mapValues {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))

    // 对value添加字符串"|||"
    rdd.mapValues(_ + "|||").collect().foreach(println)

    // 关闭连接
    sc.stop()

  }

}
