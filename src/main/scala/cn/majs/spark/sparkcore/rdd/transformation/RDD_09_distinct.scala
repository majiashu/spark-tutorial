package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子 distinct()  - 去重
 *               distinct(num)  - 去重后指定分区数
 * @author: mac 
 * @create: 2020/09/29 16:45 
 */
object RDD_09_distinct {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 4, 3, 2, 2, 1), 5)

    println("--------原始数据----------")
    numRdd.mapPartitionsWithIndex({ (index, data) =>
      println(index + "-------->" + data.mkString(","))
      data
    }).collect()

    println("-------去重-------")
    val disRdd: RDD[Int] = numRdd.distinct()
    disRdd.mapPartitionsWithIndex({ (index, data) =>
      println(index + "-------->" + data.mkString(","))
      data
    }).collect()

    println("-------去重后指定分区-------")
    val disRdd02: RDD[Int] = numRdd.distinct(3)
    disRdd02.mapPartitionsWithIndex({ (index, data) =>
      println(index + "-------->" + data.mkString(","))
      data
    }).collect()


    //4.关闭连接
    sc.stop()

  }
}
