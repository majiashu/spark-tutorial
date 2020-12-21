package cn.majs.spark.sparkcore.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program spark-atguigu 
 * @description: 转换算子 sample（withReplacement， fraction， seed）  - 随机抽样
 *                       - withReplacement:Boolean
 *                            true: 放回抽样 false：无放回抽样
 *                       - fraction：Double
 *                            当withReplacement=true时（放回抽样）：选择每个元素期望出现的次数，取值必须大于等于0
 *                            当withReplacement=false时（不放回抽样）：选着每个元素的概率，取值[0, 1]
 *                       - seed: 抽样算法初始值
 *                            一般不需要指定   其他条件一样时，seed也相同时抽取的随机说一样
 *
 *
 * @author: mac 
 * @create: 2020/09/29 13:54 
 */
object RDD_08_sample {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    // 放回实验
    rdd.sample(true, 1).collect().foreach(println)
    // 不放回
    rdd.sample(false, 0.5).collect().foreach(println)

    /**
     *  抽出指定个数的样本 takeSample（放回不放回， 个数， 种子）
     */
    val nameRdd: RDD[String] = sc.makeRDD(List("张三", "李四", "王五", "马六", "马云", "王健林", "马化腾"))
    val names: Array[String] = nameRdd.takeSample(false, 2)
    names.foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
