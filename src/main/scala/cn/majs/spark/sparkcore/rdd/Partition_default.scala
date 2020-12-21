package cn.majs.spark.sparkcore.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: RDD的默认分区数
 *              - 从集合中读取
 *                  取决于分配给应用的CPU核数   setMaster("local[*]")表示分配机器的全部核数，将*换成数字是跟换成指定核数
 *              - 从外部文件中读取
 *                  Math.min(分配给应用的CPU核数, 2)
 *
 * @author: mac 
 * @create: 2020/09/28 14:16 
 */
object Partition_default {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 通过集合创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 通过文件创建RDD
    val rdd2: RDD[String] = sc.textFile("/Users/mac/IdeaProjects/spark-atguigu/input/word.txt")

    // 打印出rdd的分区数
    println(rdd2.partitions.size)

    rdd2.saveAsTextFile("/Users/mac/IdeaProjects/spark-atguigu/output/")

    //4.关闭连接
    sc.stop()

  }

}
