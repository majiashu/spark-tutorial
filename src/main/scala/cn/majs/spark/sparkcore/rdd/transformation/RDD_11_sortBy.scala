package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子 排序 sortBy（f:(T)=>k,   ascending:Boolean=true ...）
 *                                   按什么排序        默认正序true，倒序false
 * @author: mac 
 * @create: 2020/09/29 21:34 
 */
object RDD_11_sortBy {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val numRDD: RDD[Int] = sc.makeRDD(List(1, 5, 3, 2, 6, 7, 9))

    numRDD.sortBy(num=>num).collect().foreach(println)

    numRDD.sortBy(num=>num, false).collect().foreach(println)

    val numRDD02: RDD[String] = sc.makeRDD(List("4", "7", "3", "1", "8", "2"))
    numRDD02.sortBy(num=>num.toInt).collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
