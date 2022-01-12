package com.wnn.ews.content

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.wnn.common.{ConfigUtils, DateUtils}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object GenerateTWBaseSongInfoD {
  private val hivemetastoreuris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val useDatabase: String = ConfigUtils.HIVE_DATABASE

  def getAlbumName: String => String = (name: String) => {
    //    [{"name":"《中国好声音第二季 第十五期》","id":"5ce5ffdb2a0b8b6b4707263e"}]
    var albumName = ""
    try {
      val array: JSONArray = JSON.parseArray(name)
      albumName = array.getJSONObject(0).getString("name")
    } catch {
      case e: Exception => {
        if (name != null && name.contains("《") && name.contains("》")) {
          albumName = name.substring(name.indexOf("《"), name.lastIndexOf("》") + 1)
        } else {
          albumName = "暂无专辑"
        }
      }
    }
    albumName
  }

  def getSingerInfo: (String, String, String) => String = (singerInfo: String, singer: String, idOrName: String) => {
    //    [{"name":"胡海泉","id":"10306"},{"name":"丁于","id":"6311"}]
    var singerOrId = ""
    try {
      val array: JSONArray = JSON.parseArray(singerInfo)
      if (array.size() > 0 && "singer1".equals(singer) && "id".equals(idOrName)) {
        singerOrId = array.getJSONObject(0).getString("id")
      } else if (array.size() > 0 && "singer1".equals(singer) && "name".equals(idOrName)) {
        singerOrId = array.getJSONObject(0).getString("name")
      } else if (array.size() > 1 && "singer2".equals(singer) && "id".equals(idOrName)) {
        singerOrId = array.getJSONObject(1).getString("id")
      } else if (array.size() > 1 && "singer2".equals(singer) && "name".equals(idOrName)) {
        singerOrId = array.getJSONObject(1).getString("name")
      }
    } catch {
      case e: Exception => {
        singerOrId
      }
    }
    singerOrId
  }

  def getPostTime: String => String = (postTime: String) => {
    //    2013-10-31 00:00:00 | 1.3819392E+12
    var npostTime = ""
    try {
      npostTime = DateUtils.formatDate(postTime)
    } catch {
      case e: Exception => {

      }
    }
    npostTime
  }

  def getAuthCompany: String => String = (authCompany: String) => {
    var authName = ""
    try {
      val nObject: JSONObject = JSON.parseObject(authName)
      authName = nObject.getString("name")
    } catch {
      case e: Exception => {
        authName
      }
    }

    authName
  }


  def getPrdctType: String => String = (prdctType: String) => {
    //    [8,6,9]
    var productarray = ""
    if (prdctType != null && prdctType.length > 1) {
      productarray = prdctType.stripPrefix("[").stripSuffix("]")
    }
    productarray
  }

    def main0(args: Array[String]): Unit = {
      var s="[{\"name\":\"邓健泓\",\"id\":\"14958\"}]"
      val str: String = getSingerInfo(s,"singer1","name")
      val id: String = getSingerInfo(s,"singer1","id")
      println(str)
//      println(getPrdctType("[8]"))
//      println(getAlbumName("《LANDIVA》"))
//      println(getAlbumName("[{\"name\":\"《Say The Words》\",\"id\":\"5c19a78350057d3b2023025d\"}]"))
    }

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("TWBaseSongInfo")
//      .master("local")
//      .config("hive.metastore.uris", hivemetastoreuris)
      .enableHiveSupport()
      .getOrCreate()


    session.sql(s"use $useDatabase")

    import org.apache.spark.sql.functions._ //导入函数，可以使用 udf、col 方法

    val udfGetAlbumName = udf(getAlbumName)
    val udfGetSingerInfo = udf(getSingerInfo)
    val udfGetPostTime = udf(getPostTime)
    val udfGetAuthCompany = udf(getAuthCompany)
    val udfGetPrdctType = udf(getPrdctType)
//    println("===================")
//    session.table("TO_SONG_INFO_D").show()
//    println("===================")

    session.table("TO_SONG_INFO_D")
      .withColumn("ALBUM", udfGetAlbumName(col("ALBUM")))
      .withColumn("SINGER1", udfGetSingerInfo(col("SINGER_INFO"), lit("singer1"), lit("name")))
      .withColumn("SINGER1ID", udfGetSingerInfo(col("SINGER_INFO"), lit("singer1"), lit("id")))
      .withColumn("SINGER2", udfGetSingerInfo(col("SINGER_INFO"), lit("singer2"), lit("name")))
      .withColumn("SINGER2ID", udfGetSingerInfo(col("SINGER_INFO"), lit("singer2"), lit("id")))
      .withColumn("POST_TIME", udfGetPostTime(col("POST_TIME")))
      .withColumn("AUTH_CO", udfGetAuthCompany(col("AUTH_CO")))
      .withColumn("PRDCT_TYPE", udfGetPrdctType(col("PRDCT_TYPE")))
      .createTempView("TEMP_TO_SONG_INFO_D")

    val sql: DataFrame = session.sql(
      """
  select  NBR ,
  NVL(NAME,OTHER_NAME) as NAME,
  SOURCE                   ,
  ALBUM                    ,
  PRDCT                    ,
  LANG                     ,
  VIDEO_FORMAT             ,
  DUR                      ,
  SINGER1                  ,
  SINGER2                  ,
  SINGER1ID                ,
  SINGER2ID                ,
  0 as MAC_TIME            ,
  POST_TIME                ,
  PINYIN_FST               ,
  PINYIN                   ,
  SING_TYPE                ,
  ORI_SINGER               ,
  LYRICIST                 ,
  COMPOSER                 ,
  BPM_VAL                  ,
  STAR_LEVEL               ,
  VIDEO_QLTY               ,
  VIDEO_MK                 ,
  VIDEO_FTUR               ,
  LYRIC_FTUR               ,
  IMG_QLTY                 ,
  SUBTITLES_TYPE           ,
  AUDIO_FMT                ,
  ORI_SOUND_QLTY           ,
  ORI_TRK                  ,
  ORI_TRK_VOL              ,
  ACC_VER                  ,
  ACC_QLTY                 ,
  ACC_TRK_VOL              ,
  ACC_TRK                  ,
  WIDTH                    ,
  HEIGHT                   ,
  VIDEO_RSVL               ,
  SONG_VER                 ,
  AUTH_CO                  ,
  STATE                    ,
  split(PRDCT_TYPE,',') as prdct_type
  from TEMP_TO_SONG_INFO_D
      """.stripMargin)

    //       sql.show()
//        sql.write.mode(SaveMode.Overwrite).text("/user/hive/warehouse/data/song/TW_SONG_BASEINFO_D")
//    sql.write.format("Hive").mode(SaveMode.Overwrite).saveAsTable("TW_SONG_BASEINFO_D")  //???这种方式会先删除外表再创建内表

//    sql.write.format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
//      .mode(SaveMode.Overwrite)
//      .save("/user/hive/warehouse/data/song/TW_SONG_BASEINFO_D/")

    sql.createTempView("Temp_tw_song_baseinfo_d")
    session.sql(
      """
        | insert overwrite table TW_SONG_BASEINFO_D select * from Temp_tw_song_baseinfo_d
      """.stripMargin)

    println("=========== finished =============")
  }

}

