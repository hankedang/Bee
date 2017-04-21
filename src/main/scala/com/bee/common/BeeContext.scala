package com.bee.common

import com.bee.common.util.{Logging, Utils}
import com.bee.core.exec.{BeeChannel, BeeSink, BeeSource}


/**
  * Created by han.kedang on 16/8/26.
  */


class BeeContext(beeConf : BeeConf)
  extends Logging{

  private val _conf : BeeConf = beeConf
  private val beeName = _conf.get("bee.name", null)

  def getName: String = beeName

  def createSource() : BeeSource = {
    val _type = _conf.get(s"bee.source.type","com.bee.exec.source.ReadLineSource") match {
        case "tail" => "com.bee.exec.source.ReadLineSource"
        case _ => _conf.get(s"bee.source.type","com.bee.exec.source.ReadLineSource")
    }
    info(s"Source instance: ${_type}")
    Utils.classForName(_type)
      .getDeclaredConstructor(_conf.getClass)
      .newInstance(_conf)
      .asInstanceOf[BeeSource]
  }

  def createChannel() : BeeChannel = {
    val _type = _conf.get(s"bee.channel.type","com.bee.exec.channel.QueueChannel") match {
        case "queue" => "com.bee.exec.channel.QueueChannel"
        case _ => _conf.get(s"bee.channel.type","com.bee.exec.channel.QueueChannel")
      }
    info(s"Channel instance: ${_type}")
    Utils.classForName(_type)
      .getDeclaredConstructor(_conf.getClass)
      .newInstance(_conf)
      .asInstanceOf[BeeChannel]
  }

  def createSink() : BeeSink = {
    val _type = _conf.get(s"bee.sink.type","com.bee.exec.sink.EchoSink") match {
      case "kafka" => "com.bee.exec.sink.KafkaSink"
      case "echo" => "com.bee.exec.sink.EchoSink"
      case _ => _conf.get(s"bee.sink.type","com.bee.exec.sink.EchoSink")
    }
    info(s"Sink instance: ${_type}")
    Utils.classForName(_type)
      .getDeclaredConstructor(_conf.getClass)
      .newInstance(_conf)
      .asInstanceOf[BeeSink]
  }

}

