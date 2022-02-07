package com.wnn.ews.machine

import java.util.Properties

import com.wnn.common.ConfigUtils
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object EwsMachineBaseInfo {


  def getProviceAndCity = (addr: String, realaddr: String, index: Int) => {
    //北京市朝阳区 上海市 天津市蓟州区 广西壮族自治区柳州市  广西壮族自治区钦州市 宁夏回族自治区银川市 新疆维吾尔自治区喀什地区  内蒙古自治区包头市 重庆市璧山区
    var province = addr;
    var city = ""
    if (province == null || province.equals("")) {
      province = ""
      city = ""
    } else if (province.contains("北京市")) {
      province = "北京市"
    } else if (province.contains("上海市")) {
      province = "上海市"
    } else if (province.contains("天津市")) {
      province = "天津市"
    } else if (province.contains("广西壮族自治区")) {
      province = "广西壮族自治区"
    } else if (province.contains("宁夏回族自治区")) {
      province = "宁夏回族自治区"
    } else if (province.contains("新疆维吾尔自治区")) {
      province = "新疆维吾尔自治区"
    } else if (province.contains("内蒙古自治区")) {
      province = "内蒙古自治区"
    } else if (province.contains("重庆市")) {
      province = "重庆市"
    } else {
      province = addr.substring(0, addr.indexOf("省") + 1)
    }

    if (realaddr.contains("_")) {
      val temparea: String = realaddr.substring(0, realaddr.indexOf("_"))
      city = addr.substring(province.length, addr.indexOf(temparea));
      if (city == "") {
        city = temparea;
      }
    } else {
      city = addr.substring(province.length, addr.indexOf(realaddr));
    }
    if (index == 1) province else city
  }

  def getTime = (time: String) => {
    if ("null".equals(time)) {
      "19700101000000"
    } else if (time.length == 8) {
      time + "000000"
    } else {
      time
    }
  }

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("Machine statistics")
//      .master("local")
      .config("spark.sql.shuffle.partitions", 10)
//      .config("hive.metastore.uris", "thrift://hadoop52:9083")
      .enableHiveSupport()
      .getOrCreate()


    val ycakMac: DataFrame = session.table("TO_YCAK_MAC_D")
    val ycakMacLoc: DataFrame = session.table("TO_YCAK_MAC_LOC_D")
    val ycbkAdmin: DataFrame = session.table("TO_YCBK_MAC_ADMIN_MAP_D")

    ycakMac.select("mid")
      .union(ycakMacLoc.select("mid"))
      .union(ycbkAdmin.select("machine_num"))
      .distinct()
      .createTempView("temp_all_macid")

    ////////////测试获取所有机器ID
    //  session.sql(
    //      """
    //        | select count(*) from temp_all_macid
    //      """.stripMargin).show()

    //////////////清洗TO_YCAK_MAC_LOC_D表中的省份，城市信息
    import org.apache.spark.sql.functions._

    val udfGetProvince = udf(getProviceAndCity)
    val udfgettime = udf(getTime)

    ycakMacLoc.filter(" address != 'null' and (province = 'null' or city = 'null')")
      .withColumn("province", udfGetProvince(col("address"), col("real_address"), lit(1)))
      .withColumn("city", udfGetProvince(col("address"), col("real_address"), lit(2)))
      .withColumn("revenue_time", udfgettime(col("revenue_time")))
      .withColumn("sale_time", udfgettime(col("sale_time")))
      .union(
        ycakMacLoc.filter(" address != 'null' and (province != 'null' or city != 'null')")
          .withColumn("revenue_time", udfgettime(col("revenue_time")))
          .withColumn("sale_time", udfgettime(col("sale_time")))
      )
      .createTempView("TEMP_TO_YCAK_MAC_LOC_D")

    session.sql(
      """
        | select a.mid,
        | b.serialnum ,
        | b.hard_id  ,
        | b.song_warehouse_version,
        | b.exec_version    ,
        | b.ui_version,
        | b.online  ,
        | b.status ,
        | b.current_login_time,
        | b.pay_switch   ,
        | b.language_type ,
        | b.songware_type ,
        | b.screen_type,
        |  a.province_id
        | ,a.city_id
        | ,a.province
        | ,a.city
        | ,a.map_class
        | ,a.mglng
        | ,a.mglat
        | ,a.address
        | ,a.real_address
        | ,a.revenue_time
        | ,a.sale_time
        |
        | from temp_all_macid c
        | join TEMP_TO_YCAK_MAC_LOC_D a
        | on c.mid=a.mid
        | left join TO_YCAK_MAC_D b
        | on c.mid=b.mid
      """.stripMargin)
      .createTempView("TEMP_YCAK")

    //////////////////////////////////以上是整合ycak库中的表，以下是整合ycbk库中的表

    session.sql(
      """
        |  select
        |  a.mid,
        |  b.machine_name,
        |  b.inv_rate , --投资人分成比例
        |  b.age_rate , --承接方分成比例
        |  b.com_rate , --公司分成比例
        |  b.par_rate , --合作方分成比例
        |  b.package_name, --套餐名称
        |  b.product_type,
        |  case when b.product_type='1' then 'MiniK' else 'Kshow' end  product_name,
        |  b.activate_time , --激活时间
        |  b.is_activate, --是否已激活
        |  b.had_mpay_func, --是否开通移动支付功能
        |  h.hard_id,
        |  h.song_warehouse_version,
        |  h.exec_version,
        |  h.ui_version,
        |  h.revenue_time,
        |  h.sale_time ,
        |  d.store_name, --店名，代理人，
        |  d.tag_name , --主场景
        |  d.sub_tag_name, --主场景分类
        |  d.sub_scence_name, --子场景
        |  d.sub_scence_cate_name, --子场景分类
        |  d.brand_name,
        |  d.sub_brand_name,
        |  h.province,
        |  h.city,
        |  h.address,
        |  h.real_address,
        |  h.current_login_time
        |
        |  from temp_all_macid a
        |  join to_ycbk_mac_admin_map_d b
        |  on a.mid=b.machine_num
        |  join to_ycbk_mac_store_map_d c
        |  on a.mid=c.machine_num
        |  join to_ycbk_store_d d
        |  on c.store_id=d.id
        |  join TEMP_YCAK h
        |  on h.mid= a.mid
      """.stripMargin)
      .createTempView("TEMP_TW_MAC_BASEINFO_D")

//        |  e.province,
//        |  f.city,
//        |  g.area,
//        |  join to_ycbk_privc_d e
//        |  on d.store_province_code=e.provinceid
//        |  join to_ycbk_city_d f
//        |  on d.store_city_code=f.cityid and e.provinceid=f.provinceid
//        |  join to_ycbk_area_d g
//        |  on g.cityid=f.cityid


///////////////////////////////////////   将临时表中的数据以分区形式插入hive表中，同时备份到mysql
    var datestr="20201230"
    session.sql(
      s"""
        | insert overwrite  table TW_YCABK_MAC_BASEINFO_D partition (data_dt=${datestr}) select * from TEMP_TW_MAC_BASEINFO_D
      """.stripMargin)

    ///////////////////////////////  向mysql中备份
    val properties = new Properties()
    properties.setProperty("user",s"${ConfigUtils.MYSQL_USER}")
    properties.setProperty("password",s"${ConfigUtils.MYSQL_PASSWORD}")
    properties.setProperty("driver","com.mysql.jdbc.Driver")

    session.sql(
      """
        | select * from TEMP_TW_MAC_BASEINFO_D
      """.stripMargin)
      .write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.159.52:3306/d1?useUnicode=true&characterEncoding=UTF-8","tw_ycabk_mac_baseinfo_d",properties)

    println("============= finished ==============")
  }



}
