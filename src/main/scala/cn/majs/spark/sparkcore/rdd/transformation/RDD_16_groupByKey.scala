package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:
 *              转换算子  groupByKey
 *                          - 对kv类型的数据进行操作
 *                          - 按照k重新分组
 *                          - 可设置新RDD的分区数
 *                          - (("a", 2), ("b", 3), ("a", 1), ("b", 4)) --->(a,CompactBuffer(2, 1))
 *                                                                       (b,CompactBuffer(3, 4))
 *                                                                        key   value的集合
 *
 *
 *                        reduceByKey与groupByKey的区别？
 *                          - reduceByKey:按照Key进行聚合，shuffle之前有预聚合（combine）操作
 *                          - groupByKey：按照Key进行分组，直接进行shuffle
 *                          - 在不影响业务的情况下优先使用reduceByKey。求和不影响业务逻辑；求平均值影响业务逻辑
 *
 *
 * @author: mac 
 * @create: 2020/09/30 14:39 
 */
object RDD_16_groupByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 3), ("a", 1), ("b", 4)))

    rdd.groupByKey().collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
