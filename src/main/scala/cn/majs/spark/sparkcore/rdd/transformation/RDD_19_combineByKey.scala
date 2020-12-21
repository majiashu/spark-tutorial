package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:
 *                      转换算子--->combineByKey()
 *                      - 对KV类型的RDD进行操作
 *                      - 转换结构后对分区内和分区间的操作
 *
 *                      参数说明---> combineByKey(
 *                      createCombiner:V=>C  分组内创建组合的函数，通俗点将就是对读进来的数据进行初始化，其把当前的值
 *                      作为参数，可以对该值进行转换操作，转换成我们想要的数据结构
 *                      mergeValue:(C,V)=>C   该函数主要是分区内的合并函数，作用在每一个分区内部，其功能主要是讲V合并到
 *                      之前（createCombiner）的元素C上，这里的C指的是上一个函数转换之后的数据
 *                      格式，而这里的V指的是原始数据格式（上一个函数为转换之前的）
 *                      mergeCombiners:(C,C)=>R 该函数主要是进行分区合并，此时是将两个C合并成一个C，例如两个C（Int）相加得到
 *                      一个R:(Int)
 *                      )
 * @author: mac 
 * @create: 2020/09/30 20:15 
 */
object RDD_19_combineByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /**
     * 需求说明：创建一个pairRDD，根据key计算每种key的均值。
     * (先计算每个key对应值的总和以及key出现的次数，再相除得到结果）
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    /**
     * 实现方式一： combineByKey--> map
     */
    val combineRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1), // 转换结构:a--->(88, 1)
      (tup1: (Int, Int), v) => {
        (tup1._1 + v, tup1._2 + 1) // 分区内计算a---> (88, 1), 91=> (88+91, 1+1)
      },
      (tup2: (Int, Int), tup3: (Int, Int)) => {
        (tup2._1 + tup3._1, tup2._2 + tup3._2) //分区间计算 a--->(179, 2) (95, 1)=>(274, 3)
      }
    )
    val resRDD: RDD[(String, Int)] = combineRdd.map({
      case (str, tuple) => (str, tuple._1 / tuple._2)
    })
    resRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
