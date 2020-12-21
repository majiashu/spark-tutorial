package cn.majs.spark.sparkcore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 广播变量： 分布式共享只读变量，用来高效分发较大的对象
 * @author: mac 
 * @create: 2020/10/12 15:25 
 */
object Broadcast {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /**
     * 采用RDD的方式实现 rdd1 join rdd2，用到Shuffle，性能比较低
     */
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    rdd1.join(rdd2).collect().foreach(println)

    /**
     * 采用集合的方式，实现rdd1和list的join
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6))
    // 声明广播变量
    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    val resultRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (k1, v1) => {
        var v2: Int = 0
        // 使用广播变量
        //for ((k3, v3) <- list.value) {
        for ((k3, v3) <- broadcastList.value) {
          if (k1 == k3) {
            v2 = v3
          }
        }
        (k1, (v1, v2))
      }
    }
    resultRDD.foreach(println)

    // 关闭连接
    sc.stop()

  }

}
