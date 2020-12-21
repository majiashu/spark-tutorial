package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子 sortByKey -按照Key进行排序
 *                       sortByKey(ascending:Boolean, numPartitions)
 *                                    默认升序true         分区数
 *                       key必须实现Ordered接口
 * @author: mac 
 * @create: 2020/10/06 14:09 
 */
object RDD_20_sortByKey {
  def main(args: Array[String]): Unit = {

    // 1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3具体业务逻辑
    // 3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    // 3.2 按照key的正序（默认顺序）
    rdd.sortByKey(true).collect().foreach(println)

    // 3.3 按照key的倒序
    rdd.sortByKey(false).collect().foreach(println)

    // 自定义key
    val stdList = List(
      (new Student("Ma", 18), 1),
      (new Student("Li", 18), 1),
      (new Student("Ma", 19), 1),
      (new Student("Wang", 18), 1),
      (new Student("Ma", 20), 1)
    )
    val stdRdd: RDD[(Student, Int)] = sc.makeRDD(stdList)
    stdRdd.sortByKey().collect().foreach(println)

    // 4.关闭连接
    sc.stop()
  }
}

/**
 * 自定义key排序
 */

class Student(var name:String, var age:Int) extends Ordered[Student] with Serializable {
  override def compare(that: Student): Int = {
    // 先按照名称升序排序，如果名称相同的话，再按照年龄降序排序
    var res: Int = this.name.compareTo(that.name)
    //如果名称相同按照年龄降序排序
    if(res==0){
      res = that.age - this.age
    }
    res
  }

  override def toString = s"Student($name, $age)"
}
