package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: ${TODO} 
 * @author: mac 
 * @create: 2020/09/28 18:22 
 */
object RDD_04_flatMap {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(6, 7)), 2)

    val newRdd: RDD[Int] = rdd.flatMap(item => item)  // 扁平化
    newRdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()



  }
}
