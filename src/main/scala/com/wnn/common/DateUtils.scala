package com.wnn.common

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}
import java.math.BigDecimal

object DateUtils {

  /**
    * 将字符串格式化成"yyyy-MM-dd HH:mm:ss"格式
    * @param dateString
    * @return
    */
  def formatDate(dateString:String):String ={
    //    2013-10-31 00:00:00 | 1.3819392E+12
    var datestr = ""
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      datestr = df.format(df.parse(dateString))
    } catch {
      case e:Exception =>{
        try {
          val decimal = new BigDecimal(dateString)
          val tmpD = new Date(decimal.longValue())
          datestr = df.format(tmpD)
        } catch {
          case e:Exception => {
            datestr
          }
        }
      }
    }
    datestr
  }


  /**
    * 获取输入日期的前几天的日期
    * @param datestr
    * @param dateN
    * @return
    */
  def getCurrentDatePreDate(datestr:String,dateN:Int): String ={
    var dateFormat = ""

    val df = new SimpleDateFormat("yyyyMMdd")
    val date: Date = df.parse(datestr)
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_MONTH,-dateN)

    dateFormat = df.format(calendar.getTime)
    dateFormat
  }

  def main(args: Array[String]): Unit = {
     var dstr="1.3819392E+12"
    val str: String = formatDate(dstr)
    println(str)
  }

}
