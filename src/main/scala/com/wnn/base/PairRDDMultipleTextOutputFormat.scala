package com.wnn.base

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class PairRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[String,Any]{

  override def generateFileNameForKeyValue(key: String, value: Any, name: String): String = {
    key
  }

  override def generateActualKey(key: String, value: Any): String = null
}
