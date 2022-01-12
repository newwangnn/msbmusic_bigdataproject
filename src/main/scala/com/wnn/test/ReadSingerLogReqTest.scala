package com.wnn.test

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadSingerLogReqTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("singerLog").setMaster("local")
    val sc = new SparkContext(conf)



    val fileods: RDD[String] = sc.textFile("/musictestdata/currentday_clientlog.tar.gz")
    fileods.filter(line=>{line.split("&").length==6})
      .map(line=>{
        val strs: Array[String] = line.split("&")
        (strs(2),strs(3))
      })
//      .foreach(println)

    //        {"songid": "LX_U016464",
    //        "mid": 96596,
    //        "optrate_type": 3,
    //        "uid": 0,
    //        "consume_type": 0,
    //        "play_time": 201,
    //        "dur_time": 200,
    //        "session_id": 61914,
    //        "songname": "情侣装",
    //        "pkg_id": 1,
    //        "order_id": "InsertCoin_148571"}

      .map(x=>{
        if("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(x._1)){
          val json: JSONObject = JSON.parseObject(x._2)
          val songid: String = json.getString("songid")
          val mid: String = json.getString("mid")
          val optrate_type: String = json.getString("optrate_type")
          val uid: String = json.getString("uid")
          val consume_type: String = json.getString("consume_type")
          val play_time: String = json.getString("play_time")
          val dur_time: String = json.getString("dur_time")
          val session_id: String = json.getString("session_id")
          val songname: String = json.getString("songname")
          val pkg_id: String = json.getString("pkg_id")
          val order_id: String = json.getString("order_id")
          (x._1,s"$songid\t$mid\t$optrate_type\t$uid\t$consume_type\t$play_time\t$dur_time\t$session_id\t$songname\t$pkg_id\t$order_id")
        }else{
          x
        }
      }).saveAsHadoopFile("hdfs://mycluster/musicproject/ods",classOf[String],classOf[String],classOf[MyMultipleTextOutputFormat])


  }

  class MyMultipleTextOutputFormat extends MultipleTextOutputFormat[String,String]{
    //   文件名：根据key和value自定义输出文件名。name：对应的part-0001文件名
    override def generateFileNameForKeyValue(key: String, value: String, name: String): String = {
      key
    }
    //文件内容：默认同时输出key与value.这里指定不输出key
    override def generateActualKey(key: String, value: String): String = null
  }

}
