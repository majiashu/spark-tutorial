package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子  重新分区
 *              - coalesce
 *               默认是不执行shuffle，一般用于缩减分区
 *              - repartition
 *               底层调用的就是coalesce，只不过默认执行shuffle，一般用于扩大分区
 * @author: mac 
 * @create: 2020/09/29 21:02 
 */
object RDD_10_coalesce_repartition {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    // 分区与内容一起打印
    numRdd.mapPartitionsWithIndex({ (index, data) =>
      println(index + "----->" + data.mkString(","))
      data
    }).collect()

    /**
     * 用coalesce 减少分区至2个
     */
    val coaRDD: RDD[Int] = numRdd.coalesce(2)
    // 分区与内容一起打印
    coaRDD.mapPartitionsWithIndex({ (index, data) =>
      println(index + "----->" + data.mkString(","))
      data
    }).collect()

    /**
     * 用repartition 增加分区至4个
     */
    val repRdd: RDD[Int] = numRdd.repartition(4)
    repRdd.mapPartitionsWithIndex({ (index, data) =>
      println(index + "----->" + data.mkString(","))
      data
    }).collect()


    //4.关闭连接
    sc.stop()

  }

}
