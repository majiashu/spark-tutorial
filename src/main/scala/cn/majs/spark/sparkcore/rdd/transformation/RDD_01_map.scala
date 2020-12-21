package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: ${TODO}
 * @author: mac 
 * @create: 2020/09/28 16:52 
 */
object RDD_01_map {
  def main(args: Array[String]): Unit = {

    // 1.创建SparkConf 并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val list: List[Int] = List(1, 2, 3, 4, 5, 6)

    val rdd: RDD[Int] = sc.makeRDD(list)
    rdd.collect().foreach(println)

    val mapRdd: RDD[Int] = rdd.map(_ * 2)   // map映射rdd中的元素
    mapRdd.collect().foreach( println)

    //4.关闭连接
    sc.stop()

  }

}
