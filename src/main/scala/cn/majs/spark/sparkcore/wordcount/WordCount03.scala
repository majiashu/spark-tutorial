package cn.majs.spark.sparkcore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 实现wordcount
 *               List（("hello spark", 2), ("hello scala", 3), ("hello word", 2))）  =>  ("hello", 2) ("spark", 2)
 *               RDD: flatMap --> groupBy --> map
 * @author: mac 
 * @create: 2020/09/29 11:44 
 */
object WordCount03 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello spark", 2), ("hello scala", 3), ("hello word", 2)))

    /**
     * 扁平化处理
     * (hello,2)
     * (spark,2)
     * (hello,3)
     * (scala,3)
     * (hello,2)
     * (word,2)
     */
    val flatMapRdd: RDD[(String, Int)] = rdd.flatMap({ case (words, count) =>
      words.split(" ").map(word => (word, count))
    })

    /**
     * 按照元组中第一个元素进行分组
     * (hello,CompactBuffer((hello,2), (hello,3), (hello,2)))
     * (word,CompactBuffer((word,2)))
     * (spark,CompactBuffer((spark,2)))
     * (scala,CompactBuffer((scala,3)))
     */
    val groupByRdd: RDD[(String, Iterable[(String, Int)])] = flatMapRdd.groupBy(_._1)

    /**
     * (hello,7)
     * (word,2)
     * (spark,2)
     * (scala,3)
     */
    val resRdd: RDD[(String, Int)] = groupByRdd.map({ case (word, data) => {
      (word, data.map(_._2).sum)
    }
    })

    resRdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
