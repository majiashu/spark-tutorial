package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子 - 双value类型
 *                          - 合集
 *                             union
 *                          - 交集
 *                             intersection
 *                          - 差集
 *                             subtract
 *                          - 拉链
 *                              zip  每个分区中个数应该一样
 * @author: mac 
 * @create: 2020/09/29 21:59 
 */
object RDD_13_doubleValue {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(4, 5, 6, 7))

    //合集
    rdd1.union(rdd2).collect().foreach(println)

    //交集
    rdd1.intersection(rdd2).collect().foreach(println)

    //差集
    rdd1.subtract(rdd2).collect().foreach(println)
    rdd2.subtract(rdd1).collect().foreach(println)

    //拉链  对应分区的个数相同 不同会报错
    rdd1.zip(rdd2).collect().foreach(println)


    //4.关闭连接
    sc.stop()

  }

}
