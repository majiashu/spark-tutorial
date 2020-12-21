package cn.majs.spark.sparkcore.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 行动算子  reduce
 * @author: mac 
 * @create: 2020/10/06 20:17 
 */
object RDD_01_action {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 3, 6, 8))

    // 行动算子---> reduce
    val reduceRDD: Int = rdd.reduce(_ + _)
    println(reduceRDD)

    // 行动算子---> collect 以数组形式返回数据
    val collectRDD: Array[Int] = rdd.collect()
    collectRDD.foreach(println)

    // 行动算子---> foreach
    rdd.foreach(println)

    // 行动算子---> count 返回RDD中的元素个数
    val countRDD: Long = rdd.count()
    println(countRDD)

    // 行动算子---> first 返回RDD中第一个元素
    val firstRDD: Int = rdd.first()
    println(firstRDD)

    // 行动算子---> take  返回RDD中前N个元素的数组
    val takeRDD: Array[Int] = rdd.take(3)
    println(takeRDD.mkString(","))

    // 行动算子---> takeOrdered   取排序后的前N个元素的数组
    val takeOrRDD: Array[Int] = rdd.takeOrdered(3)
    println(takeOrRDD.mkString(","))

    // 行动算子---> aggregate  首先分区内 初始值与分区内操作， 然后 初始值与分区间操作
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)
    val aggregateRDD: Int = rdd1.aggregate(0)(_ + _, _ + _)
    println(aggregateRDD)

    // 行动算子---> fold aggregate的简化 分区内分区间规则相同
    val foldRDD: Int = rdd1.fold(10)(_ + _)
    println(foldRDD)

    // 行动算子---> countByKey  统计相同Key的个数
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c"), (3, "c")))
    val countByKeyRDD: collection.Map[Int, Long] = rdd2.countByKey()
    println(countByKeyRDD)

    // 行动算子---> save相关
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 保存文本格式
    rdd3.saveAsTextFile("/Users/mac/IdeaProjects/spark-atguigu/output01")
    // 保存序列化文件
    rdd3.saveAsObjectFile("/Users/mac/IdeaProjects/spark-atguigu/output02")
    // 保存为SequenceFile   只支持KV类型
    rdd3.map((_, 1)).saveAsSequenceFile("/Users/mac/IdeaProjects/spark-atguigu/output03")
    // 可以读进来 textFile()  objectFile() sequenceFile() 等
    sc.textFile("/Users/mac/IdeaProjects/spark-atguigu/output01")


    // 关闭连接
    sc.stop()

  }

}
