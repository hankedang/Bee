package com.bee.core.exec

import com.bee.common.BeeConf
import com.bee.common.util.Logging
import com.bee.core.Data
import com.bee.core.exception.BeeException


/**
  * Created by han.kedang on 16/8/27.
  */
abstract class BeeChannel(conf:BeeConf)
  extends Logging {

  val name = conf.get("bee.name", null)
  if(name == null) {
    throw new BeeException("Bee Name must be set in your configuration")
  }

  val capacity = conf.getInt(s"bee.${name}.channel.capacity", 10 * 1000)
  var _size: Long = 0

  type T = Data

  protected def doPut(data: T)

  protected def doTake: T

  protected def doClear

  def size : Long = this._size


  def put(data: T): Unit = {
    doPut(data)
    this._size += 1
  }

  def take(): T = {
    val data = doTake
    this._size -= 1
    data
  }

  def clear = doClear


}
