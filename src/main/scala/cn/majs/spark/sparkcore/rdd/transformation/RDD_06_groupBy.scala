package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子groupBy
 *               - 按照传入函数的返回值进行分组，将相同的key放在同一个迭代器中
 *
 * @author: mac 
 * @create: 2020/09/28 21:16 
 */
object RDD_06_groupBy {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // %2相同的在一组
    rdd.groupBy(_%2).collect().foreach(println)   // (0,CompactBuffer(2, 4))  (1,CompactBuffer(1, 3))

    val rdd2: RDD[String] = sc.makeRDD(List("scala", "hive", "spark", "hive"))
    // 首字母相同的分在一组
    rdd2.groupBy(str=>str.substring(0, 1)).collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
