package com.wnn.dm.content

import java.util.Properties

import com.wnn.common.ConfigUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object GenerateTmSingerRsiD {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("Generate tm song rsi d")
//      .master("local")
//      .config("hive.metastore.uris", ConfigUtils.HIVE_METASTORE_URIS)
      .config("spark.sql.shuffle.partitions", "1")
      .enableHiveSupport().getOrCreate()

    val currentDate="20201230"

    session.sql(s"use ${ConfigUtils.HIVE_DATABASE}")
    val df: DataFrame = session.sql(
      s"""
         | select data_dt,singer1id,singer1,
         | sum(singcnt) as sing_cnt,
         | sum(suppcnt) as supp_cnt,
         | sum(sing7cnt) as rct_7_sing_cnt,
         | sum(supp7cnt) as rct_7_supp_cnt,
         | sum(dtop7singcnt) as rct_7_top_sing_cnt,
         | sum(dtop7suppcnt) as rct_7_top_supp_cnt,
         | sum(sing30cnt) as rct_30_sing_cnt,
         | sum(supp30cnt) as rct_30_supp_cnt,
         | sum(dtop30singcnt) as rct_30_top_sing_cnt,
         | sum(dtop30suppcnt) as rct_30_top_supp_cnt
         | from tw_song_ftur_d
         | where data_dt=${currentDate}
         | group by data_dt,singer1id,singer1
      """.stripMargin)

    import org.apache.spark.sql.functions._

    df.withColumn("RSI_1D",pow(
      log(col("SING_CNT")/1+1)*0.63*0.8+log(col("SUPP_CNT")+1)*0.63*0.2
      ,2)*10)
      .withColumn("RSI_7D",pow(
        (log(col("RCT_7_SING_CNT")/7+1)*0.63+log(col("RCT_7_SUPP_CNT")+1)*0.37)*0.8
          +
          (log(col("RCT_7_TOP_SING_CNT")/7+1)*0.63+log(col("RCT_7_TOP_SUPP_CNT")+1)*0.37)*0.2
        ,2)*10)
      .withColumn("RSI_30D",pow(
        (log(col("RCT_30_SING_CNT")/30+1)*0.63+log(col("RCT_30_SUPP_CNT")+1)*0.37)*0.8
          +
          (log(col("RCT_30_TOP_SING_CNT")/30+1)*0.63+log(col("RCT_30_TOP_SUPP_CNT")+1)*0.37)*0.2
        ,2)*10).createTempView("TEMP_TW_SONG_FTUR_D")

    val rsi_1d = session.sql(
      s"""
         | select
         |  "1" as PERIOD,SINGER1ID,SINGER1,RSI_1D as RSI,
         |  row_number() over(partition by data_dt order by RSI_1D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)

    val rsi_7d = session.sql(
      s"""
         | select
         |  "7" as PERIOD,SINGER1ID,SINGER1,RSI_7D as RSI,
         |  row_number() over(partition by data_dt order by RSI_7D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)

    val rsi_30d = session.sql(
      s"""
         | select
         |  "30" as PERIOD,SINGER1ID,SINGER1,RSI_30D as RSI,
         |  row_number() over(partition by data_dt order by RSI_30D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)

    rsi_1d.union(rsi_7d).union(rsi_30d).createTempView("result")


    session.sql(
      """
        | select * from result
      """.stripMargin).show()

    session.sql(
      s"""
         |insert overwrite table TW_SINGER_RSI_D partition(data_dt=${currentDate}) select * from result
      """.stripMargin)

    /**
      * 这里把 排名前30名的数据保存到mysql表中
      */
    val properties  = new Properties()
    properties.setProperty("user",s"${ConfigUtils.MYSQL_USER}")
    properties.setProperty("password",s"${ConfigUtils.MYSQL_PASSWORD}")
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    session.sql(
      s"""
         | select ${currentDate} as data_dt,PERIOD,SINGER1ID,SINGER1,RSI,RSI_RANK from result where rsi_rank <=30
      """.stripMargin).write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.159.52:3306/d1?useUnicode=true&characterEncoding=UTF-8","tm_singer_rsi",properties)
    println("**** all finished ****")
  }

}
