package com.bee.exec.source

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import com.bee.common.BeeConf
import com.bee.common.util.Utils
import com.bee.core.Data
import com.bee.core.exec.BeeSource

/**
  * Created by han.kedang on 16/8/25.
  */
class ReadLineSource(conf: BeeConf)
  extends BeeSource(conf: BeeConf) {

  private val _file = s"${_path}/${_name}"
  private var rdf = new RandomReadFile(s"${_path}/${_name}")

  override def changeFile: Unit = {

    val currentTime = Utils.getDateStr(_format)
    val fileName = s"${_path}/${_name}-${_launchTime}.log"
    val file = new File(fileName)

    if(file.exists) {
      if(rdf != null)
        rdf.close
      _launchTime = currentTime
      rdf = new RandomReadFile(s"${_file}")
      info(s"change source file name to : ${_file}")
    } else {
      // Nothing to do
      Thread.sleep(1000)
    }
  }

  override def read(): T = if(!rdf.hasNext) rdf.next else null

  override def skip(l: Long): Unit = {
    rdf.skip(l)
    info(s"${_path} need skip : ${l} ")
  }

  override def close: Unit =
    rdf.close

  override def getPosition: Long = rdf.getPosition

}

private class RandomReadFile (fileName : String)  {

  private[this] val fis = new FileInputStream(fileName)
  private[this] val isr = new InputStreamReader(fis)
  private[this] val br = new BufferedReader(isr)

  // 当前读取的位置, 行号
  private[this] var position : Long = 0

  private[this] var line = ""

  def skip(lineNum : Long) = {
    for(tmp <- 1L to lineNum){
      br.readLine()
      isLastLine
    }
  }

  def getPosition = position

  def hasNext: Boolean = {
    line = br.readLine
    isLastLine
  }

  def next : Data = {
    Data.buildData(this.position, line)
  }

  def isLastLine : Boolean = {
    if(line == null) true
    else {
      position += 1
      false
    }
  }

  def close = {
    br.close
    isr.close
    fis.close
  }
}
