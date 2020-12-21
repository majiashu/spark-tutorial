package cn.majs.spark.sparkcore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:
 * 实现wordcount map-->flatMap--> groupBy --> map
 * @author: mac 
 * @create: 2020/09/29 10:54 
 */
object WordCount02 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 复杂场景的wordcount 此时列表中元素是元组，第二个数是出现的次数
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello spark", 2), ("hello scala", 3), ("hello word", 2)))

    // * 将该RDD转换成常见的形式
    val rdd1: RDD[String] = rdd.map({ case (str, count) => {
      (str + " ") * count
    }
    })

    val flatMapRdd: RDD[String] = rdd1.flatMap(_.split(" "))

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
