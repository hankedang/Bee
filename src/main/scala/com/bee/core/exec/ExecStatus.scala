package com.bee.core.exec

/**
  * Created by han.kedang on 16/9/5.
  */


class ExecStatus {
  import StatusEnum._

  private var _status = StatusEnum.STOP
  private var _who: String = null

  def setStatus(status: StatusEnum): Unit = {
    this._status = status
  }

  def setWho(who: String) = this._who = who

  def getStatus = this._status

  def who = this._who

}


object StatusEnum extends Enumeration{
  type StatusEnum = Value

  val STARTING, RUNNING, SHUTDOWNING, STOP, EXCEPTIONSTOP = Value


}
