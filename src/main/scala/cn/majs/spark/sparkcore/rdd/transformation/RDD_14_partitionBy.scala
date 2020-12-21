package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description:   转换算子 partitionBy
 *                           - 对kv类型RDD重新分区
 *                           - RDD本身是没有partitionBy这个算子的，它是通过隐式转换动态的给kv类型的RDD扩展的功能
 *                           - 可自定义Partitioner 如下面所示
 * @author: mac 
 * @create: 2020/09/30 10:46 
 */
object RDD_14_partitionBy {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 2), (2, 3), (3, 4), (1,3)), 3)
    //带分区号打印
    rdd.mapPartitionsWithIndex({(index, data)=>
      println(index+"----->"+data.mkString(","))
      data
    })collect()

    val newRdd: RDD[(Int, Int)] = rdd.partitionBy(new HashPartitioner(2))
    //带分区号打印
    newRdd.mapPartitionsWithIndex({(index, data)=>
      println(index+"----->"+data.mkString(","))
      data
    })collect()

    /**
     * 使用自定义分区
     */
    val newRdd02: RDD[(Int, Int)] = rdd.partitionBy(new MyPartitioner(2))
    //带分区号打印
    newRdd02.mapPartitionsWithIndex({(index, data)=>
      println(index+"----->"+data.mkString(","))
      data
    })collect()

    //4.关闭连接
    sc.stop()
  }

  /**
   * 自定义分区器,可参照HashPartitioner去编写
   *
   */
  class MyPartitioner(partitions: Int) extends Partitioner{

    //获取分区个数
    override def numPartitions: Int = partitions
    //返回值int表示分区编号，从0开始
    override def getPartition(key: Any): Int = {
      if(key ==1){
        0
      }else{
        1
      }
    }
  }

}
