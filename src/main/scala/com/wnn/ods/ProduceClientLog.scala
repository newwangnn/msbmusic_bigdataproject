package com.wnn.ods


import com.alibaba.fastjson.{JSON, JSONObject}
import com.wnn.base.PairRDDMultipleTextOutputFormat
import com.wnn.common.ConfigUtils
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 读取 运维人员每天上传到服务器上的客户端日志，进行解析，并加载到 ODS层 的表
  *     加载的ODS表如下：
  *
  *  注意:运行此类需要指定参数是 指定当前日期，格式：20201231 ,
  *   第二个参数是指定对应的日志保存的目录，例如：hdfs://mycluster/logdata/currentday_clientlog.tar.gz ,
  *     本地直接指定：./MusicProject/data/currentday_clientlog.tar.gz
  */
object ProduceClientLog {

  private val localrun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDataBase = ConfigUtils.HIVE_DATABASE
  private var sparkSession : SparkSession = _
  private var sc: SparkContext = _
  private val hdfsclientlogpath : String = ConfigUtils.HDFS_CLIENT_LOG_PATH
  private var clientLogInfos : RDD[String] = _

  def main(args: Array[String]): Unit = {
    /**
      * 先判断有没有传递一个参数
      *  指定当前log数据的日期时间 格式：20201231
      *  2.指定当前log的上传路径：例如 : ./MusicProject/data/currentday_clientlog.tar.gz
      */
//    if(args.length<1){
//      println(s"需要指定 数据日期")
//      System.exit(1)
//    }
//    val logDate = args(0) // 日期格式 ： 年月日 20201231
    val logDate = "20201230" // 日期格式 ： 年月日 20201231

    if(localrun){
      sparkSession = SparkSession.builder()
        .master("local")
        .appName("ProduceClientLog")
        .config("hive.metastore.uris",hiveMetaStoreUris).enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile(
        s"${hdfsclientlogpath}/currentday_clientlog.tar.gz")
      println(s"local=============$sparkSession")
    }else{
      sparkSession = SparkSession.builder().appName("ProduceClientLog")
        .config("spark.sql.warehouse.dir","/user/hive/warehouse")
        .enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile(s"${hdfsclientlogpath}/currentday_clientlog.tar.gz")
      println(s"yarn=======${args(0)}======$sparkSession")
    }

    //组织K,V格式的数据 ： （客户端请求类型，对应的info信息）
    val tableNameAndInfos = clientLogInfos.map(line => line.split("&"))
      .filter(item => item.length == 6)
      .map(line => (line(2), line(3)))

    //获取当日出现的所有的“请求类型”
    //    val allTableNames = tableNameAndInfos.keys.distinct().collect()


    /*
1575302461&92126&MINIK_CLIENT_ADVERTISEMENT_RECORD&{"src_verison": 2558, "mid": 92126, "adv_type": 4, "src_type": 2558, "uid": 0, "session_id": 34294, "event_id": 1, "time": 1575302460}&3.0.1.15&2.4.4.30
1575302461&96755&MINIK_CLIENT_SONG_PLAY_OPERATE_REQ&{"songid": "lx149965", "mid": 96755, "optrate_type": 0, "uid": 0, "consume_type": 0, "play_time": 0, "dur_time": 0, "session_id": 74960, "songname": "年少有为", "pkg_id": 100, "order_id": ""}&3.0.1.15&2.4.4.30
1575302461&96755&MINIK_SERVER_GET_SONG_SELECT_LIST_REQ&{"songs": ["lx149965"], "uid": 0, "type": 0}&3.0.1.15&2.4.4.30
1575302461&96755&MINIK_CLIENT_REPORT_PKG_USE_STATUS&{"mid": 96755, "uid": 0, "time_stamp": 1575302460, "status": 1, "session_id": 24, "order_id": "InsertCoin_172356"}&3.0.1.15&2.4.4.30
1575302461&96755&MINIK_SERVER_GET_SONG_SELECT_LIST_REQ&{"songs": [], "uid": 0, "type": 0}&3.0.1.15&2.4.4.30
1575302461&85651&MINIK_CLIENT_ADVERTISEMENT_RECORD&{"src_verison": 2546, "mid": 85651, "adv_type": 4, "src_type": 2546, "uid": 0, "session_id": 51428, "event_id": 1, "time": 1575302459}&3.0.1.15&2.4.4.30

     */

    //转换数据，将数据分别以表名的方式存储在某个路径中
//   MINIK_CLIENT_SONG_PLAY_OPERATE_REQ
//   {"songid": "lx149965", "mid": 96755, "optrate_type": 0, "uid": 0, "consume_type": 0, "play_time": 0, "dur_time": 0, "session_id": 74960,
    //   "songname": "年少有为", "pkg_id": 100, "order_id": ""}
    tableNameAndInfos.map(tp=>{
      val tableName = tp._1//客户端请求类型
      val tableInfos = tp._2//请求的json string
      if("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(tableName)){
        val jsonObject: JSONObject = JSON.parseObject(tableInfos)
        val songid = jsonObject.getString("songid") //歌曲ID
        val mid = jsonObject.getString("mid") //机器ID
        val optrateType = jsonObject.getString("optrate_type") //0:点歌, 1:切歌,2:歌曲开始播放,3:歌曲播放完成,4:录音试听开始,5:录音试听切歌,6:录音试听完成
        val uid = jsonObject.getString("uid") //用户ID（无用户则为0）
        val consumeType = jsonObject.getString("consume_type")//消费类型：0免费；1付费
        val durTime = jsonObject.getString("dur_time") //总时长单位秒（operate_type:0时此值为0）
        val sessionId = jsonObject.getString("session_id") //局数ID
        val songName = jsonObject.getString("songname") //歌曲名
        val pkgId = jsonObject.getString("pkg_id")//套餐ID类型
        val orderId = jsonObject.getString("order_id") //订单号
        (tableName,songid+"\t"+mid+"\t"+optrateType+"\t"+uid+"\t"+consumeType+"\t"+durTime+"\t"+sessionId+"\t"+songName+"\t"+pkgId+"\t"+orderId)
      }else{
        //将其他表的infos 信息直接以json格式保存到目录中
        tp
      }
    })
      .saveAsHadoopFile(
      s"${hdfsclientlogpath}/all_client_tables/${logDate}",
      classOf[String],
      classOf[String],
      classOf[PairRDDMultipleTextOutputFormat]
    )

    println(s"yarn=======${hdfsclientlogpath}======${logDate}")

    /**
      * 在Hive中创建 ODS层的 TO_CLIENT_SONG_PLAY_OPERATE_REQ_D 表
      */
    sparkSession.sql(s"use $hiveDataBase ")
    sparkSession.sql(
      """
        | CREATE EXTERNAL TABLE IF NOT EXISTS `TO_CLIENT_SONG_PLAY_OPERATE_REQ_D`(
        |  `SONGID` string,  --歌曲ID
        |  `MID` string,     --机器ID
        |  `OPTRATE_TYPE` string,  --操作类型
        |  `UID` string,     --用户ID
        |  `CONSUME_TYPE` string,  --消费类型
        |  `DUR_TIME` string,      --时长
        |  `SESSION_ID` string,    --sessionID
        |  `SONGNAME` string,      --歌曲名称
        |  `PKG_ID` string,        --套餐ID
        |  `ORDER_ID` string       --订单ID
        | )
        | partitioned by (data_dt string)
        | ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        | LOCATION 'hdfs://mycluster/user/hive/warehouse/data/song/TO_CLIENT_SONG_PLAY_OPERATE_REQ_D'
      """.stripMargin)

    sparkSession.sql(
      s"""
         | load data inpath
         | '${hdfsclientlogpath}/all_client_tables/${logDate}/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ'
         | into table TO_CLIENT_SONG_PLAY_OPERATE_REQ_D partition (data_dt='${logDate}')
      """.stripMargin)

    println("**** all finished ****")
  }
}
