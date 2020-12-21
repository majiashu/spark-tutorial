package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子  cogroup 类似全连接，但是在同一个RDD中对key聚合
 *                               操作两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合
 *
 *
 *                                 ((1,"a"),(2,"b"),(3,"c"))        (1,(CompactBuffer(a),CompactBuffer(4)))
 *                                                           --->   (2,(CompactBuffer(b),CompactBuffer(5,1)))
 *                                 ((1,4),(2,5),(3,6)(2,1))         (3,(CompactBuffer(c),CompactBuffer(6)))
 * @author: mac 
 * @create: 2020/10/06 19:01 
 */
object RDD_23_cogroup {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))

    // 创建第二个RDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(3,6),(2,1)))

    // cogroup两个RDD并打印结果
    rdd.cogroup(rdd1).collect().foreach(println)



    // 关闭连接
    sc.stop()

  }
}
