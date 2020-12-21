package cn.majs.spark.sparkcore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 实现WordCount
 *                groupByKey ----> map
 * @author: mac 
 * @create: 2020/09/30 14:54 
 */
object WordCount04 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 1), ("b", 4)))

    val groupRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey() //(a,CompactBuffer(2, 1))  (b,CompactBuffer(3, 4))

    val resRDD: RDD[(String, Int)] = groupRdd.map({
      case (key, data) => {
        (key, data.sum)
      }
    })

    resRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
