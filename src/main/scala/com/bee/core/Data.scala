package com.bee.core

import com.bee.common.util.Utils

/**
  * Created by han.kedang on 16/9/2.
  */

object Data {

  def buildData(index: Long, data: Any): Data = {
    new Data(index, data)
  }

}

class Data(index: Long, data: Any) {

  val i = index
  val d = data
  val timestamp = Utils.timestamp

  override def toString: String = {
    s"index: ${i}, data: ${d}, timestamp: ${timestamp}"
  }

}
