package cn.majs.spark.sparkcore.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 从文件中读取数据创建RDD
 * @author: mac
 * @create: 2020/09/28 10:18
 */
object CreateRdd_File {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("CreateRdd_File").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    //从文件中读取
    val rdd: RDD[String] = sc.textFile("/Users/mac/IdeaProjects/spark-atguigu/input/word.txt")

    //从HDFS中读取数据
    val rdd2: RDD[String] = sc.textFile("hdfs://node01:9000/input")

    rdd2.collect().foreach(println)
    
    sc.stop()

  }
}
