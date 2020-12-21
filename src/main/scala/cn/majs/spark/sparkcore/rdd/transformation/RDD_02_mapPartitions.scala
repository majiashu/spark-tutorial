package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: ${TODO}
 * @author: mac 
 * @create: 2020/09/28 17:11 
 */
object RDD_02_mapPartitions {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val newRdd: RDD[Int] = rdd.mapPartitions(x => x.map(_ * 2))   // mapPartitions按分区去映射rdd中元素 x为某个分区的所有元素

    newRdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
