package com.wnn.ews.content

import com.wnn.common.{ConfigUtils, DateUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object GenerateTwSongFturD {

  private val hivedatabase: String = ConfigUtils.HIVE_DATABASE
  private val hivemetastoreuris: String = ConfigUtils.HIVE_METASTORE_URIS

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("< song feature seven and thrity >")
//      .master("local")
//      .config("hive.metastore.uris", hivemetastoreuris)
      .enableHiveSupport()
      .getOrCreate()

    session.sql(s"use $hivedatabase")

    var currentDate = "20201230"
    var current7Date =DateUtils.getCurrentDatePreDate("20201230",7)
    var current30Date =DateUtils.getCurrentDatePreDate("20201230",30)

println(s"============== $currentDate::$current7Date::$current30Date================")

    session.sql(
      s"""
        | select
        | 	songid as NBR,
        | 	count(*) as singCNT,
        | 	0 as suppCNT,
        | 	count(distinct uid) as userCNT,
        | 	count(distinct order_id) as orderIdCNT
        | from to_client_song_play_operate_req_d
        | where data_dt='20201230'
        | group by songid
      """.stripMargin)
      .withColumn("flagDate",lit("1"))
        .createTempView("CurrentSingInfo")



    session.sql(
      s"""
        | select
        | 	songid as NBR,
        | 	count(*) as sing7CNT,
        | 	0 as supp7CNT,
        | 	count(distinct uid) as user7CNT,
        | 	count(distinct order_id) as orderId7CNT
        | from to_client_song_play_operate_req_d
        | where data_dt between  ${current7Date}   and  ${currentDate}
        | group by songid
      """.stripMargin)
      .withColumn("flagDate",lit("7"))
      .createTempView("Current7SingInfo")


    session.sql(
      s"""
         | select
         | 	songid as NBR,
         | 	count(*) as sing30CNT,
         | 	0 as supp30CNT,
         | 	count(distinct uid) as user30CNT,
         | 	count(distinct order_id) as orderId30CNT
         | from to_client_song_play_operate_req_d
         | where data_dt between  ${current30Date}   and  ${currentDate}
         | group by songid
      """.stripMargin)
      .withColumn("flagDate",lit("30"))
      .createTempView("Current30SingInfo")

    import org.apache.spark.sql.functions._

    val df1: DataFrame = session.table("CurrentSingInfo")
    val df7: DataFrame = session.table("Current7SingInfo")
    val df30: DataFrame = session.table("Current30SingInfo")
    df1.union(df7).union(df30).createTempView("current1and7and30")


    session.sql(
      """
        | select  NBR,
        | max(case when flagDate="7" then singCNT else 0 end ) as DTop7SingCNT,
        | max(case when flagDate="7" then suppCNT else 0 end ) as DTop7SuppCNT,
        | max(case when flagDate="30" then singCNT else 0 end ) as DTop30SingCNT,
        | max(case when flagDate="30" then suppCNT else 0 end ) as DTop30SuppCNT
        | from  current1and7and30
        | group by NBR
      """.stripMargin)
      .createTempView("TOP7and30SingInfo")


//    session.sql(
//      s"""
//         | select
//         | NBR,
//         | 	max( case when data_dt between ${current7Date} and ${currentDate} then SING_CNT else 0 end ) as  DTop7SingCNT,
//         | 	max( case when data_dt between ${current7Date} and ${currentDate} then SUPP_CNT else 0 end ) as  DTop7SuppCNT,
//         | max(SING_CNT) as DTop30SingCNT,
//         | max(SUPP_CNT) as DTop30SuppCNT
//         | from TW_SONG_FTUR_D
//         | where data_dt between  ${current30Date}   and  ${currentDate}
//         | group by NBR
//      """.stripMargin)
//      .createTempView("TOP7and30SingInfo")


    session.sql(
      s"""
        |  select
        |  A.NBR,
        |  B.name,B.source,B.album,B.prdct,B.lang,B.video_format,B.dur,B.singer1,B.singer1id,B.singer2,B.singer2id,B.mac_time,
        |  A.singCNT,A.suppCNT,A.userCNT,A.orderIdCNT,
        |  nvl(C.sing7CNT,0) as  sing7CNT,
        |  nvl(C.supp7CNT,0) as  supp7CNT,
        |  nvl(C.user7CNT,0) as  user7CNT,
        |  nvl(C.orderId7CNT,0) as  orderId7CNT,
        |  nvl(D.sing30CNT,0) as  sing30CNT,
        |  nvl(D.supp30CNT,0) as  supp30CNT,
        |  nvl(D.user30CNT,0) as  user30CNT,
        |  nvl(D.orderId30CNT,0) as  orderId30CNT,
        |  nvl(E.DTop7SingCNT,0) as  DTop7SingCNT,
        |  nvl(E.DTop7SuppCNT,0) as  DTop7SuppCNT,
        |  nvl(E.DTop30SingCNT,0) as  DTop30SingCNT,
        |  nvl(E.DTop30SuppCNT,0) as  DTop30SuppCNT
        |  from CurrentSingInfo A
        |  left join  TW_song_baseinfo_d B on A.NBR=B.NBR
        |  left join Current7SingInfo C on A.NBR=C.NBR
        |  left join Current30SingInfo D on A.NBR=D.NBR
        |  left join TOP7and30SingInfo E on A.NBR=E.NBR
      """.stripMargin)
      .createTempView("result")
    session.table("result").show(100)

    session.sql(
      s"""
        |  insert overwrite table tw_song_ftur_d partition (data_dt=${currentDate}) select * from result

      """.stripMargin)

    println("======== finish ============")


  }

}
