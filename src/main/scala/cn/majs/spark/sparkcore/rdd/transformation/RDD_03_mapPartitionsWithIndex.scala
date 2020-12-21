package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:  mapPartitionsWithIndex 按分区去映射带分区号
 * @author: mac 
 * @create: 2020/09/28 17:19 
 */
object RDD_03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)  //3指RDD分区数是3

    // 分区号与元素组成元组
    val newRdd: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, items) => {
      items.map((index, _))
    })
    newRdd.collect().foreach(println)

    //第二个分区的元素*2
    val newRdd2: RDD[Int] = rdd.mapPartitionsWithIndex((index, item) => { //index：分区号  item：对应分区的元素
      index match {     //match  类似 switch
        case 1 => item.map(_ * 2)
        case _ => item
      }
    })
    newRdd2.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
