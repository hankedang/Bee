package com.bee.exec.channel

import java.util.concurrent.LinkedBlockingQueue

import com.bee.common.BeeConf
import com.bee.core.Data
import com.bee.core.exec.BeeChannel

/**
  * Created by han.kedang on 16/8/31.
  */

class QueueChannel(conf: BeeConf)
  extends BeeChannel(conf: BeeConf) {

  private val queue = new LinkedBlockingQueue[Data](capacity)

  override def doPut(data: T): Unit = {
    queue.put(data)
    val rate = size * 100/capacity
    if(rate > 90) {
      warn("Queue more then 90%, wait 1000ms.")
      Thread.sleep(1000)
    } else if(rate > 98) {
      warn("Queue more then 98%, wait 5000ms. Plaese check sink is or not normal")
      Thread.sleep(5000)
    }

  }

  override def doTake: T = {
    queue.poll()
  }

  override def doClear: Unit = {
    queue.clear()
  }


}


