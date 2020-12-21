package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:            转换算子 filter
 *                          - 按照指定过滤规则，对RDD中的元素进行过滤
 * @author: mac 
 * @create: 2020/09/29 13:42 
 */
object RDD_07_filter {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("scala", "java", "spark", "hadoop", "ha", "hive"))
    val filterRdd: RDD[String] = rdd.filter(_.contains("ha")) // 过滤出包含"ha"的字符串
    filterRdd.collect().foreach(println)

    val rdd02: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val filterRdd02: RDD[Int] = rdd02.filter(_ % 2 != 0)  // 过滤出奇数
    filterRdd02.collect().foreach(println)


    //4.关闭连接
    sc.stop()

  }

}
