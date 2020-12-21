package cn.majs.spark.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
 * @program spark-atguigu 
 * @description: 自定义累加器
 *               自定义累加器步骤
 *               （1）继承AccumulatorV2，设定输入、输出泛型
 *               （2）重写方法
 *
 *               需求：自定义累加器，统计集合中首字母为“H”单词出现的次数。
 *               List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark")
 * @author: mac 
 * @create: 2020/10/12 14:55 
 */
object Accumulator_Define {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark", "Hadoop"))
    // 创建累加器
    val accumulator1: MyAccumulator = new MyAccumulator()
    // 注册累加器
    sc.register(accumulator1,"wordcount")
    // 使用累加器
    rdd.foreach(
      word =>{
        accumulator1.add(word)
      }
    )
    // 获取累加器的累加结果
    println(accumulator1.value)

    // 关闭连接
    sc.stop()

  }

}

/**
 * 自定义累加器
 *
 * // 声明累加器
 * // 1.继承AccumulatorV2,设定输入、输出泛型
 * // 2.重新方法
 */

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

  // 定义输出数据集合
  var map = mutable.Map[String, Long]()

  // 是否为初始化状态，如果集合数据为空，即为初始化状态
  override def isZero: Boolean =  map.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {new MyAccumulator}

  // 重置累加器
  override def reset(): Unit = map.clear()

  // 增加数据
  override def add(elem: String): Unit = {
    // 业务逻辑
    if(elem.startsWith("H")){
      map(elem) = map.getOrElse(elem, 0L) + 1
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    var map1 = map
    var map2 = other.value
    map = map1.foldLeft(map2)(
      (map,kv)=>{
        map(kv._1) = map.getOrElse(kv._1, 0L) + kv._2
        map
      }
    )

  }

  // 累加器的值
  override def value: mutable.Map[String, Long] = map
}



