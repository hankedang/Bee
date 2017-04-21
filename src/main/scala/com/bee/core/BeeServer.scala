package com.bee.core

import java.util.concurrent.atomic.AtomicBoolean
import java.io._

import com.bee.common.BeeConf
import com.bee.common.util._
import net.sf.json.{JSONArray}

import scala.collection.concurrent.TrieMap

/**
  * Created by han.kedang on 16/8/30.
  */

object BeeServer {
  private val bs = new BeeServer

  def startup: Int = bs.startup

  def startSchedule(name: String): Int = bs.startScheduler(name)

  def shutdownAll: Unit = bs.shutdownAll

  def shutdown(name: String) = bs.shutdown(name)

  def queryStatus = bs.queryStatus

}


class BeeServer private
  extends Logging{

  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)
  // 记录已经启动的 schedule
  private val schedulers =  new TrieMap[String, BeeScheduler]


  private val config: BeeConf = new BeeConf(true)
  //
  val offsetDir = config.get("bee.offset.dir", null)
  info(s"offset dir: ${offsetDir}")
  val offsetFile = new File(offsetDir)
  if(!offsetFile.exists()){
    info("offser dir not exists! create it")
    offsetFile.mkdirs()
  }


  def startup: Int = {
    try {
      info("starting bee ....")

      if(isShuttingDown.get)
        throw new IllegalStateException("Bee server is still shutting down, cannot re-start!")

      if(startupComplete.get)
        return 1

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        // start bee scheduler
        startAllScheduler
      }
      info(s"All Bee start complete!")
      return 0
    }
    catch {
      case e : Throwable =>
        error("startup fail. Prepare to shutdown", e)
        isStartingUp.set(false)
        throw e
    }
    -1
  }

  def shutdown(name: String) = {
    val result = schedulers.get(name)
    result match {
      case None => warn(s"${name} scheduler is not exist or has been closed !")
      case _ => {
        val sd = result.get
        sd.shutdown
        schedulers -= (name)
        info(s"${name} scheduler has been closed.")
      }
    }

  }

  def shutdownAll = {
    schedulers.foreach(x => {
      x._2.shutdown
      schedulers -= (x._1)
    })
    schedulers.clear()
  }



  private def startAllScheduler = {
    val names = config.get("bee.name", null)
    if(names == null) {
      throw new Throwable("Bee Name must be set in your configuration")
    }

    val arrName: Array[String] = names.split(";")

    arrName.foreach(name => startScheduler(name))

  }

  private def startScheduler(name: String): Int = {
    val arrName: Array[String] = config.get("bee.name", null).split(";")

    if(!arrName.contains(name)) {
      error(s"${name} does not exists. please check config file. ")
      return -1
    }

    if(schedulers.contains(name)) {
      error(s"${name} already started. palease don't start it again!")
      -1
    } else {
      val schduler = new BeeScheduler(config.clone(name))
      Utils.newThread(s"start-${name}", schduler).start()
      schedulers += (name -> schduler)
      info(s"start ${name} scheduler .")
      0
    }

  }


  private def queryStatus: JSONArray = {
    val arr = new JAHelper
    schedulers.map(x => {
      /**
        *
        *   {
        *     name : [
        *       source:[
        *         {status: running},
        *         {position: 100}
        *       ],
        *       channel:[
        *         {size: 100}
        *       ],
        *       sink:[
        *         {status: running},
        *         {offset: 100}
        *       ]
        *     ],
        *     .....
        *   }
        *
        *
        */
      arr.add(new JOHelper().put(x._1, new JAHelper()
        .add(new JOHelper()
          .put("source", new JAHelper()
            .add(new JOHelper()
              .put("status", x._2.getSource.getStatus.getStatus.toString)
              .put("position", x._2.getSource.getPosition)
              .getJSONObject
            ).getJSONArray).getJSONObject)
        .add(new JOHelper()
          .put("channel", new JAHelper()
            .add(new JOHelper()
              .put("size", x._2.getChannel.size)
              .getJSONObject
            ).getJSONArray).getJSONObject)
        .add(new JOHelper()
          .put("sink", new JAHelper()
            .add(new JOHelper()
              .put("status", x._2.getSink.getStatus.getStatus.toString)
              .put("offset", x._2.getOffset())
              .getJSONObject
            ).getJSONArray).getJSONObject).getJSONArray
      ).getJSONObject)
    })
    arr.getJSONArray
  }



}
