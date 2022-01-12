package com.wnn.test

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

object MyDataFrame {

  val myLenFun = (s:String)=>{
    s.length
  }

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("test my sql")
      .master("local")
      .getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    session.sparkContext.setLogLevel("error")

//    两种设置udf方式
    val myudf: UserDefinedFunction = udf(myLenFun)
//    val myudf: UserDefinedFunction = session.udf.register("myudf",myLenFun)

    val list: List[String] = List[String]("xixi","haha","miaomiao")
    val df: DataFrame = list.toDF("name")
//    df.show()显示结果
    df.withColumn("age",lit(18)) //添加列literal
//    df.createTempView("person") 创建本地表
    df.withColumn("nameLen",myudf(col("name")))

//    session.sql(
//      """
//        | select * from person
//      """.stripMargin).show()


  }
}
