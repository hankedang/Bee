package com.bee.core.exec

import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicBoolean

import com.bee.common.BeeConf
import com.bee.common.util.{Logging, Utils}
import com.bee.core.Data
import com.bee.core.exception.BeeException


/**
  * Created by han.kedang on 16/8/27.
  */


abstract class BeeSource(beeConf: BeeConf)
  extends Callable[ExecStatus] with Logging {

  val _format = "yyyyMMddHH"
  val _name = beeConf.get("bee.name", null)
  var _channel : BeeChannel = _
  val _path = beeConf.get("bee.source.path", null)
  var _launchTime = Utils.getDateStr(_format)

  private val _status = new ExecStatus
  private val isShuttingDown = new AtomicBoolean(false)


  type T = Data

  if(_path == null)
    throw new BeeException("Bee path must be set in your configuration")

  if(_name == null)
    throw new BeeException("Bee Name must be set in your configuration")

  def changeFile

  def setChannel(channel: BeeChannel) = this._channel = channel

  def read(): T

  def skip(l : Long)

  def close

  def getStatus = this._status

  def getPosition: Long

  override def call(): ExecStatus = {
    Thread.currentThread().setName(s"Source-${_name}")
    info("launch source thread. ")
    try {
      _status.setWho(s"${_name}-Source")
      _status.setStatus(StatusEnum.RUNNING)
      while (!isShuttingDown.get) {
        val data = read
        if(data == null) changeFile
        else {
          _channel.put(data)
          info(s"${data}")
        }
      }
      _status.setStatus(StatusEnum.STOP)
    }
    catch {
      case e: BeeException =>
        _status.setStatus(StatusEnum.EXCEPTIONSTOP)
        error(s"${_name} Source running exception, path: ${_path}", e)
    } finally {
      close
    }
    return _status
  }

  def shutdown = {
    isShuttingDown.set(true)
  }


}



