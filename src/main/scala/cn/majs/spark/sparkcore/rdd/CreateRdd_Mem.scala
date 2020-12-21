package cn.majs.spark.sparkcore.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:  从内存中读取数据创建RDD
 *
 * tips：scala中 .var 可以快速写变量名称
 * @author: mac
 * @create: 2020/09/27 17:35
 */
object CreateRdd_Mem {
  def main(args: Array[String]): Unit = {

    // Spark的conf配置相关，可设置AppName和Master
    val conf: SparkConf = new SparkConf().setAppName("CreateRdd_Mem").setMaster("local[*]")

    // 新建一个SparkContext
    val sc: SparkContext = new SparkContext(conf)

    val list: List[Int] = List(1, 2, 3, 4, 5)

    // 根据集合创建RDD 方式一
    //val rdd: RDD[Int] = sc.parallelize(list)

    // 根据集合创建RDD 方式二
    val rdd2: RDD[Int] = sc.makeRDD(list)

    rdd2.collect().foreach(println)

    // 关闭
    sc.stop()

  }
}
