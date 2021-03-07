package cn.majs.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @program spark-atguigu 
 * @description: ${TODO} 
 * @author: mac 
 * @create: 2020/10/23 16:10 
 */
object SparkSQLDemo {


  def main(args: Array[String]): Unit = {
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLDemo")

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  }

}
