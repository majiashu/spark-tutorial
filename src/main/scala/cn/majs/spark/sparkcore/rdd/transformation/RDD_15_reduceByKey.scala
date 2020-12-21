package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子  reduceByKey
 *                          - 对kv类型的RDD进行计算
 *                          - 按照相同的key对v进行聚合
 *                          - 可以重新指定分区数
 *
 * @author: mac
 * @create: 2020/09/30 14:31 
 */
object RDD_15_reduceByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 1), ("b", 4)), 3)
    //带分区号打印
    rdd.mapPartitionsWithIndex({(index, data)=>
      println(index+"----->"+data.mkString(","))
      data
    })collect()

    val reduceByKeyRdd: RDD[(String, Int)] = rdd.reduceByKey(_ + _, 2)
    //带分区号打印
    reduceByKeyRdd.mapPartitionsWithIndex({(index, data)=>
      println(index+"----->"+data.mkString(","))
      data
    })collect()

    //4.关闭连接
    sc.stop()

  }

}
