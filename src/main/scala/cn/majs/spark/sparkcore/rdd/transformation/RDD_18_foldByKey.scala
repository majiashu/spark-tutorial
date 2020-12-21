package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:
 *          转换算子 foldByKey()
 *                    - 对KV类型的RDD进行操作
 *                    - 根据key对分区内及分区间的数据进行操作
 *                    - 与aggregateByKey相比foldByKey分区内分区间计算规则相同
 *                    - foldByKey(初始值)（分区内和间计算规则）
 * @author: mac 
 * @create: 2020/09/30 17:31 
 */
object RDD_18_foldByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    //打印各分区对应的数
    rdd.mapPartitionsWithIndex((index, data)=>{
      println(index+"------>"+data.mkString(","))
      data
    }).collect()

    //reduceByKey 如果分区与分区间计算逻辑相同，同时不需要指定初始值优先使用 reduceByKey
    rdd.reduceByKey(_+_).collect().foreach(println)

    //foldByKey  如果分区内与分区间计算规则相同，同时需要指定初始值，优先使用foldByKey
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    //aggregateByKey  如果分区内与分区间计算逻辑不同，同时需要指定初始值，优先使用aggregateByKey
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    
    //4.关闭连接
    sc.stop()

  }

}
