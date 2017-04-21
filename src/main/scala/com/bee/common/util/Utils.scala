package com.bee.common.util

import java.io._
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.time.Period
import java.util.{Calendar, Date, Properties, Timer, TimerTask}

import scala.collection.JavaConverters._
import scala.collection.Map

/**
  * Created by han.kedang on 16/8/26.
  */


object Utils extends Logging{

  def getSystemProperties: Map[String, String] = {
    val map = System.getProperties.stringPropertyNames()
    map.asScala.map(key => (key, System.getProperty(key))).toMap
  }

  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("BEE_HOME").map { t => s"$t${File.separator}conf" }
      .map { t => new File(s"$t${File.separator}bee.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orElse(Some(getClass.getResource("/").getPath() + "../classes/bee.conf"))
      .orNull
  }

  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map(
        k => (k, properties.getProperty(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new FileNotFoundException("Failed when loading Bee Config from " + filename)
    } finally {
      inReader.close()
    }
  }

  def classForName(className : String) : Class[_] = {
    Class.forName(className)
  }

  private def newThread(name : String, runnable: Runnable, daemon: Boolean): Thread = {
    val thread = new Thread(runnable, name)
    thread.setDaemon(daemon)
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        val n = t.getName
        error(s"Uncaught exception in thread ${n} :", e)
      }
    })
    thread
  }

  def newThread(name: String, runnable: Runnable): Thread =
    newThread(name, runnable, false)


  def daemonThread(name: String, runnable: Runnable): Thread =
    newThread(name, runnable, true)


  def getLocalIP(): String =
    InetAddress.getLocalHost.getHostAddress

  def getDateStr(format: String): String = {
    val _format: SimpleDateFormat = new SimpleDateFormat(format)
    val date = Calendar.getInstance
    _format.format(date.getTime)
  }


  def timestamp = new Date().getTime




}
