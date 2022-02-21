package com.wnn.ews.user

import java.util.Properties

import com.wnn.common.DateUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//统计用户当日及7日活跃信息
object GenerateTwuserinfo {




  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("user logging")
//      .master("local")
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .enableHiveSupport()
      .getOrCreate()
//    session.sparkContext.parallelize(Seq(1))

    session.sql("use default")



    //1. 整理各个数据来源表,之后union到一起
    //"1" AS REG_CHNL,  --登录注册模式: 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
    var alipay = session.sql(
      """
        | select
        | uid     		 	    ,
        | reg_mid  			    ,
        | sex      			    ,
        | birthday  			    ,
        | msisdn as phone_number  ,
        | locationid  	        ,
        | "alipay" as reg_channel ,
        | regist_time  	        ,
        | user_exp    	        ,
        | score      		        ,
        | user_level 		    	,
        | is_certified  	        ,
        | is_student_certified    ,
        | openid                  ,
        | "" as wxid              ,
        | "" as appid             ,
        | "" as qqid
        | from to_user_alipay_info_d
      """.stripMargin)

    var wechat = session.sql(
      """
        |  select
        |  uid     		 	    ,
        |  reg_mid  			    ,
        |  sex      			    ,
        |  birthday  			    ,
        |  msisdn as phone_number  ,
        |  locationid  	        ,
        |  "wechat" as reg_channel ,
        |  regist_time  	        ,
        |  user_exp    	        ,
        |  score      		        ,
        |  user_level 		    	,
        |  null as is_certified  	        ,
        |  null as is_student_certified    ,
        |  "" as openid            ,
        |  wxid                    ,
        |  "" as appid             ,
        |  "" as qqid
        |  from to_user_wechat_info_d
      """.stripMargin)

    var qq = session.sql(
      """
        | select
        |  uid     		 	  ,
        |  reg_mid  			      ,
        |  sex      			      ,
        |  birthday  			      ,
        |  msisdn as phone_number    ,
        |  locationid  	          ,
        |  "qq" as reg_channel    	  ,
        |  regist_time  	          ,
        |  user_exp    	          ,
        |  score      		          ,
        |  user_level 		    	  ,
        |  "" as is_certified  	  ,
        |  "" as is_student_certified,
        |  "" as openid              ,
        |  "" as wxid                ,
        |  "" as appid               ,
        |  openid as qqid
        |  from to_user_qq_info_d
      """.stripMargin)

    var app = session.sql(
      """
        | select
        | uid     		 	      ,
        | reg_mid  			      ,
        | sex      			      ,
        | birthday  			      ,
        | phone_number              ,
        | locationid  	          ,
        | "app" as reg_channel      ,
        | regist_time  	          ,
        | user_exp    	          ,
        | "" as score      		  ,
        | user_level 		    	  ,
        | "" as is_certified  	  ,
        | "" as is_student_certified,
        | "" as openid              ,
        | "" as wxid                ,
        | app_uid as appid          ,
        | "" as qqid
        | from to_user_app_info_d
        |
      """.stripMargin)

    var allUserRegInfo = app.union(qq).union(wechat).union(alipay)

    // 2.  查出当天登录的用户，并将用户登录信息与用户注册信息关联起来
    var currentDate = "20201230"
    val preDate: String = DateUtils.getCurrentDatePreDate(currentDate,7)
//    var userLogin = session.table("to_user_login_info_d ")
//        .where(s"data_dt=${currentDate}")
//      .select("uid")
//      .distinct()

    session.table("to_user_login_info_d")
      .where(s"data_dt=${currentDate}")
      .join(allUserRegInfo,Seq("uid"),"left")
      .createTempView("currentUserLoggin")

//     session.table("currentUserLoggin").show()

//    3.查出当天登录信息及用户信息并插入DWS层
    session.sql(
      """
        | select
        |uid ,
        |id ,
        |mid ,
        |logintime,
        |logouttime ,
        |case when mode_type=0 then 'alipay' when mode_type=1 then 'wechat' when mode_type=3 then 'qq' else 'app' end ,
        |reg_mid  			    ,
        |sex      			    ,
        |birthday  			    ,
        |phone_number  ,
        |locationid  	        ,
        |reg_channel ,
        |regist_time  	        ,
        |user_exp    	        ,
        | cast(score as int)   ,
        |user_level 		    	,
        |is_certified  	        ,
        |is_student_certified    ,
        |openid                  ,
        |wxid              ,
        |appid             ,
        |qqid
        |from  currentUserLoggin
      """.stripMargin).createTempView("TempCurrentUserLoggin")

    session.sql(
      s"""
        | insert overwrite table tw_user_login_d partition (data_dt=${currentDate}) select * from TempCurrentUserLoggin
      """.stripMargin).createTempView("EveryDayResult")

    session.sql(
      """
        | msck repair table tw_user_login_d
      """.stripMargin)

    //TODO  以下代码分布式计算时不起作用，本地运行可以
    //4.将连接7日登录用户结果存入mysql中
    val prop: Properties = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    prop.setProperty("driver","com.mysql.jdbc.Driver")

    session.sql(
      s"""
        |  select
        |   b.*
        |  from
        |   (select uid,count(uid) c from tw_user_login_d
        |   where data_dt between ${preDate}  and ${currentDate}
        |   group by uid
        |   having c=1
        |   ) a,tw_user_login_d b
        |   where a.uid=b.uid and a.c=1 and reg_mid is not null
      """.stripMargin).write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.159.52:3306/d1?useUnicode=true&characterEncoding=UTF-8","tw_user_login_7_D",prop)

    println("============= finished ==============")
  }
}
