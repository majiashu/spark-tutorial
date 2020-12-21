package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:
 *          转换算子 aggregateByKey()
 *                    - 对KV类型的RDD进行操作
 *                    - 根据key对分区内及分区间的数据进行操作
 *                    - aggregateByKey(初始值)（分区内计算规则，分区间计算规则）
 * @author: mac 
 * @create: 2020/09/30 17:31 
 */
object RDD_17_aggregateByKey {
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

    //用aggregateByKey实现WordCount
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    //用aggregateByKey实现  分区内取出做大值，分区间相加
    rdd.aggregateByKey(0)(        //rdd:  1------>(b,3),(c,6),(c,8)  0------>(a,3),(a,2),(c,4)
      (x,y)=>math.max(x,y),   // 初始值与相同Key的Value迭代比较
      (a,b)=> a+b
    ).collect().foreach(println)

    //(简化)  用aggregateByKey实现  分区内取出做大值，分区间相加
    rdd.aggregateByKey(0)(        //rdd:  1------>(b,3),(c,6),(c,8)  0------>(a,3),(a,2),(c,4)
      math.max(_, _),   // 初始值与相同Key的Value迭代比较
      _+_
    ).collect().foreach(println)


    //4.关闭连接
    sc.stop()

  }

}
