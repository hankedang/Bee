package com.bee.exec.sink


import java.io._

import com.bee.common.BeeConf
import com.bee.core.exec.BeeSink


/**
  * Created by han.kedang on 16/8/31.
  */
class EchoSink(conf: BeeConf)
  extends BeeSink(conf: BeeConf){

  override def execute(data: T): Long = {

    println(data)

    data.i

  }


}
