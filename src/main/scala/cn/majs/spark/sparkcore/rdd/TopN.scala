package cn.majs.spark.sparkcore.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 广告点击取出每个省份的点击次数Top3的广告
 *
 *                                        map                       reduceByKey
 *       时间戳 省份id 城市id 用户id 广告id --------> (省份id-广告id，1）------------>(省份A-广告A, 100) （省份A-广告B, 1000）
 *          map                                               groupByKey
 *       -------->(省份A, (广告A， 100))  (省份A, (广告B， 1000))----------->(省份, Iterable[(广告A, 100),(广告B, 200)])
 *       mapValues
 *       --------->  (省份, Iterable[(广告B, 200), (广告A, 100)])
 *
 * @author: mac 
 * @create: 2020/10/06 19:30 
 */
object TopN {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 读取外部文件生成RDD  外部RDD结构：时间戳 省份id 城市id 用户id 广告id
    val logRDD: RDD[String] = sc.textFile("/Users/mac/IdeaProjects/spark-atguigu/input/agent.log")

    // 对数据进行map转换   转换成==>（省份id-广告id，1）
    val mapRDD: RDD[(String, Int)] = logRDD.map(
      line => {
        val fileds: Array[String] = line.split(" ")
        (fileds(1) + "-" + fileds(4), 1)
      }
    )

    // 进行聚合操作 （省份id-广告id，1）==> (省份A-广告A, 100) （省份A-广告B, 1000）
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    // 再对结构进行转换  (省份A-广告A, 100) （省份A-广告B, 1000）==> (省份A, (广告A， 100))  (省份A, (广告B， 1000))
    val map1RDD: RDD[(String, (String, Int))] = reduceRDD.map({
      case (proAndAd, clickCount) => {
        val proAndAdARR: Array[String] = proAndAd.split("-")
        (proAndAdARR(0), (proAndAdARR(1), clickCount))
      }
    })

    // 按照省份进行分组  (省份A, (广告A， 100))  (省份A, (广告B， 1000)) ==>(省份, Iterable[(广告A, 100),(广告B, 200)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = map1RDD.groupByKey()

    // 对values中的广告根据点击次数降序排前三
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      itr => {
        itr.toList.sortWith({
          (left, right) => {
            left._2 > right._2
          }
        }).take(3)
      }
    )

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()

  }

}
