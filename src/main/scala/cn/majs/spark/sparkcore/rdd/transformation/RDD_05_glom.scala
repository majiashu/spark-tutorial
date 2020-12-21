package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子glom
 *              将Rdd中分区元素组成一个数组，数组中元素类型与原分区中元素一致
 * @author: mac 
 * @create: 2020/09/28 21:16 
 */
object RDD_05_glom {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val maxRdd: RDD[Int] = rdd.glom().map(_.max)  //  1, 2 | 3, 4  =>  List(1, 2), List(3, 4)

    maxRdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
