package cn.majs.spark.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 传输数据需要序列化 Serializable
 * @author: mac 
 * @create: 2020/10/06 21:21 S
 */
object TestSerializable {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val user1 = new User()
    user1.name = "zhangsan"

    val user2 = new User()
    user2.name = "lisi"

    val userRDD1: RDD[User] = sc.makeRDD(List(user1, user2))

    userRDD1.foreach(user => println(user.name))

    // 关闭连接
    sc.stop()

  }

}

//  创建对象 继承Serializable
class User extends Serializable {
  var name: String = _

  override def toString = s"User($name)"
}
