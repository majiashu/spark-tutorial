package cn.majs.spark.sparkcore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: ${TODO}
 *              实现Wordcount  flatMap-->map-->groupBy-->map
 * @author: mac 
 * @create: 2020/09/29 10:54 
 */
object WordCount00 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala", "hello word"))

    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRdd: RDD[(String, Int)] = flatMapRdd.map((_, 1)) // (hello, 1) (spark, 1)

    // 按照元组中第一个元素groupBy分组  (hello,CompactBuffer((hello,1), (hello,1), (hello,1)))
    val groupByRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupBy(_._1)

    val resRdd: RDD[(String, Int)] = groupByRdd.map ({ case (word, data) => {
      (word, data.size)
    }
    })

    resRdd.collect().foreach(println)


    //4.关闭连接
    sc.stop()

  }
}
