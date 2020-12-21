package cn.majs.spark.sparkcore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:
 * 实现wordcount flatMap--> groupBy --> map
 * @author: mac 
 * @create: 2020/09/29 10:54 
 */
object WordCount01 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello scala", "hello word"))

    val flatMapRdd: RDD[String] = rdd.flatMap(_.split(" "))

    val groupByRdd: RDD[(String, Iterable[String])] = flatMapRdd.groupBy(word => word)

    val resRdd: RDD[(String, Int)] = groupByRdd.map({ case (word, data) => {
      (word, data.size)
    }
    })

    resRdd.collect().foreach(println)


    //4.关闭连接
    sc.stop()

  }
}
