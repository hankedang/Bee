package com.bee.core.exec

import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicBoolean

import com.bee.common.BeeConf
import com.bee.common.util.{Logging, Utils}
import com.bee.core.Data
import com.bee.core.exception.BeeException
import com.bee.core.offset._
import java.io.File

/**
  * Created by han.kedang on 16/8/27.
  */
abstract class BeeSink(conf: BeeConf)
  extends Callable[ExecStatus] with Logging {

  val _format = "yyyyMMddHH"
  var _channel: BeeChannel = _
  val isShuttingDown = new AtomicBoolean(false)
  val name = conf.get("bee.name", null)
  var _launchTime = Utils.getDateStr(_format)
  val offsetDir = conf.get("bee.offset.dir", null)
  val offsetname = s"${offsetDir}/${name}-offset"
  val file = new File(offsetname)
  if(!file.exists()){
    info(s"offset name : ${offsetname}")
    file.createNewFile()
  }
  val offsetMgr = new OffsetMgr(file)

  private val _status = new ExecStatus

  type T = Data

  if(name == null) {
    throw new BeeException("Bee Name must be set in your configuration")
  }

  def setChannel(channel: BeeChannel) = this._channel = channel

  private def setOffset(offset: Long): Unit = {
    val now = Utils.getDateStr(_format)
    offsetMgr.write(now + "|" + offset.toString)
  }

  def execute(data: T): Long

  def getStatus = this._status

  private def doExecute: Unit = {
    val data = _channel.take
    if(data != null) {
      val offset = execute(data)
      setOffset(offset)
    } else {
      if (_launchTime != Utils.getDateStr(_format)) {
        _launchTime = Utils.getDateStr(_format)
      }
      Thread.sleep(1000)
    }

  }

  override def call(): ExecStatus = {
    Thread.currentThread().setName("Sink-Thread")
    info("launch Sink thread.")
    try {
      _status.setWho(s"${name}-Sink")
      _status.setStatus(StatusEnum.RUNNING)
      while (!isShuttingDown.get) {
        doExecute
      }
      _status.setStatus(StatusEnum.STOP)
    }
    catch {
      case e: BeeException =>
        _status.setStatus(StatusEnum.EXCEPTIONSTOP)
        error(s"${name} Sink running exception", e)
    }
    _status
  }

  def shutdown = {
    val canShutdown = isShuttingDown.compareAndSet(false, true)
    if(canShutdown) {
      isShuttingDown.set(true)
    }
  }

}
