package cn.majs.spark.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 累加器
 * @author: mac 
 * @create: 2020/10/12 14:44 
 */
object Accumulator01 {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 创建RDD
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))
    // 打印单词出现的次数（a,10） 代码执行了shuffle
    dataRDD.reduceByKey(_ + _).collect().foreach(println)

    /**
     * 不shuffle，不使用累加器直接累加count是0，因为不同Executor之间无法累加
     */
    var sum = 0
    // 打印是在Executor端
    dataRDD.foreach {
      case (a, count) => {
        sum = sum + count
        println("sum=" + sum)
      }
    }
    // 打印是在Driver端
    println(("a", sum))

    /**
     * 不shuffle，使用累加器能得到正确结果，count为10
     */
    // 使用累加器实现数据的聚合功能
    // Spark自带常用的累加器
    // 声明累加器
    val sum1: LongAccumulator = sc.longAccumulator("sum1")
    dataRDD.foreach{
      case (a, count)=>{
        //  使用累加器
        sum1.add(count)
      }
    }
    // 获取累加器
    println(sum1.value)

    // 关闭连接
    sc.stop()

  }

}
