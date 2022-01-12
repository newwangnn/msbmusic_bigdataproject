package com.wnn.dm.content

import java.util.Properties

import com.wnn.common.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

object GenerateTmSongRsiD {

  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDataBase = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  private val mysqlUrl = ConfigUtils.MYSQL_URL
  private val mysqlUser = ConfigUtils.MYSQL_USER
  private val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    //    if(args.length < 1) {
    //      println(s"请输入数据日期,格式例如：年月日(20201231)")
    //      System.exit(1)
    //    }

    if (localRun) {
      sparkSession = SparkSession.builder()
        .master("local")
        .appName("Generate_TM_Song_Rsi_D")
        .config("spark.sql.shuffle.partitions", "1")
        .config("hive.metastore.uris", hiveMetaStoreUris)
        .enableHiveSupport().getOrCreate()
      sparkSession.sparkContext.setLogLevel("Error")
    } else {
      sparkSession = SparkSession.builder().appName("Generate_TM_Song_Rsi_D").enableHiveSupport().getOrCreate()
    }

    var currentDate = "20201230" //年月日 20201230

    sparkSession.sql(s"use $hiveDataBase ")

    val dataFrame = sparkSession.sql(
      s"""
         | select
         |   data_dt,                  --日期
         |   NBR,                      --歌曲ID
         |   NAME,                     --歌曲名称
         |   SINGCNT,                 --当日点唱量
         |   SUPPCNT,                 --当日点赞量
         |   SING7CNT,           --近七天点唱量
         |   SUPP7CNT,           --近七天点赞量
         |   dTOP7SINGCNT,       --近七天最高日点唱量
         |   dTOP7SUPPCNT,       --近七天最高日点赞量
         |   SING30CNT,          --近三十天点唱量
         |   SUPP30CNT,          --近三十天点赞量
         |   dTOP30SINGCNT,      --近三十天最高日点唱量
         |   dTOP30SUPPCNT       --近三十天最高日点赞量
         | from TW_SONG_FTUR_D
         | where data_dt = ${currentDate}
      """.stripMargin)

    import org.apache.spark.sql.functions._
    /**
      * 日周期-整体影响力
      * 7日周期-整体影响力
      * 30日周期-整体影响力
      */
    dataFrame.withColumn("RSI_1D", pow(
      log(col("SINGCNT") / 1 + 1) * 0.63 * 0.8 + log(col("SUPPCNT") / 1 + 1) * 0.63 * 0.2
      , 2) * 10)
      .withColumn("RSI_7D", pow(
        (log(col("SING7CNT") / 7 + 1) * 0.63 + log(col("dTOP7SINGCNT") + 1) * 0.37) * 0.8
          +
          (log(col("SUPP7CNT") / 7 + 1) * 0.63 + log(col("dTOP7SUPPCNT") + 1) * 0.37) * 0.2
        , 2) * 10)
      .withColumn("RSI_30D", pow(
        (log(col("SING30CNT") / 30 + 1) * 0.63 + log(col("dTOP30SINGCNT") + 1) * 0.37) * 0.8
          +
          (log(col("SUPP30CNT") / 30 + 1) * 0.63 + log(col("dTOP30SUPPCNT") + 1) * 0.37) * 0.2
        , 2) * 10)
      .createTempView("TEMP_TW_SONG_FTUR_D")

    val rsi_1d = sparkSession.sql(
      s"""
         | select
         |  "1" as PERIOD,NBR,NAME,RSI_1D as RSI,
         |  row_number() over(partition by data_dt order by RSI_1D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)
    val rsi_7d = sparkSession.sql(
      s"""
         | select
         |  "7" as PERIOD,NBR,NAME,RSI_7D as RSI,
         |  row_number() over(partition by data_dt order by RSI_7D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)
    val rsi_30d = sparkSession.sql(
      s"""
         | select
         |  "30" as PERIOD,NBR,NAME,RSI_30D as RSI,
         |  row_number() over(partition by data_dt order by RSI_30D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)

    rsi_1d.union(rsi_7d).union(rsi_30d).createTempView("result")

    //insert into table TW_SONG_RSI_D partition(data_dt=${currentDate}) select * from result
    sparkSession.sql(
      s"""
         |insert overwrite table TW_SONG_RSI_D partition(data_dt=${currentDate}) select * from result
      """.stripMargin)

    /**
      * 这里把 排名前30名的数据保存到mysql表中
      */
    val properties = new Properties()
    properties.setProperty("user", mysqlUser)
    properties.setProperty("password", mysqlPassword)
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${currentDate} as data_dt,PERIOD,NBR,NAME,RSI,RSI_RANK from result where rsi_rank <=30
      """.stripMargin).write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.159.52:3306/d1?useUnicode=true&characterEncoding=UTF-8", "tm_song_rsi", properties)
    println("**** all finished ****")

  }
}