package com.bee.common

import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentHashMap

import com.bee.common.util.Utils


/**
  * Created by han.kedang on 16/8/26.
  */
class BeeConf(loadDefaults : Boolean) {

  private val settings = new ConcurrentHashMap[String, String]

  if(loadDefaults) {
    loadFromSystemProperties
    loadDefaultProperties(this, null)
  }

  private def loadFromSystemProperties(): BeeConf = {
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("bee.")) {
      set(key, value)
    }
    this
  }

  private def loadDefaultProperties(conf: BeeConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(Utils.getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      Utils.getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("bee.")
      }.foreach { case (k, v) =>
        conf.set(k, v)
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }

  private def set(key: String, value: String): BeeConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  def get(key : String, defaultValue : String) : String =
    getOption(key).getOrElse(defaultValue)

  def getInt(key : String, defaultValue : Int) : Int =
    getOption(key).map(_.toInt).getOrElse(defaultValue)

  def getLong(key : String, defaultValue : Long) : Long =
    getOption(key).map(_.toLong).getOrElse(defaultValue)

  def getDouble(key : String, defaultValue : Double) : Double =
    getOption(key).map(_.toDouble).getOrElse(defaultValue)

  def getBoolean(key : String, defaultValue : Boolean) : Boolean =
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)

  private def getOption(key: String): Option[String] =
    Option(settings.get(key))


  override def clone(): BeeConf = {
    val cloned = new BeeConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue())
    }
    cloned
  }

  def clone(name: String): BeeConf = {
    val cloned = new BeeConf(false)
    settings.entrySet().asScala.foreach(e=>{
      if(e.getKey.contains(name)) {
        cloned.set(e.getKey.replace(s"${name}.", ""), e.getValue)
      }
    })
    cloned.set("bee.name", name)
    cloned.set("bee.offset.dir", settings.get("bee.offset.dir"))
    cloned
  }

}
