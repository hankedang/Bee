package com.bee.core

import java.io.File
import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, Future}

import com.bee.common.{BeeConf, BeeContext}
import com.bee.common.util.Logging
import com.bee.core.exception.BeeException
import com.bee.core.exec._
import com.bee.core.offset.OffsetMgr


/**
  * Created by han.kedang on 16/8/31.
  */



class BeeScheduler(conf: BeeConf)
  extends Runnable with Logging {
  private val threads = 2
  private val _format = "yyyyMMddHH"
  private val beeContext: BeeContext = new BeeContext(conf)
  private val beeName = beeContext.getName

  private val isShuttingDown = new AtomicBoolean(false)
  private var executor: ExecutorService = null
  private var source: BeeSource = null
  private var channel: BeeChannel = null
  private var sink: BeeSink = null
  //
  private val futures: Array[Future[ExecStatus]] = new Array[Future[ExecStatus]](2)
  private var sourceStatus: ExecStatus = null
  private var sinkStatus: ExecStatus = null

  private val timer = new Timer(beeName)

  override def run(): Unit = {
    if (!isShuttingDown.get) {
      startup
    }
  }

  def startup(): Unit = {
    try {
      debug("Initializing scheduler!")
      this synchronized {
        if (isStarted)
          throw new IllegalStateException(s"${beeName} scheduler has already been started!")
        executor = Executors.newFixedThreadPool(threads)
        //
        info(s"start ${beeName}-source thread, monitor file : ${conf.get(s"bee.source.path", null)}")
        source = beeContext.createSource()
        info(s"init channel .")
        channel = beeContext.createChannel()
        info(s"start ${beeName}-sink thread.")
        sink = beeContext.createSink()
        // launch on Source thread
        runSource
        // launch on Sink thread
        runSink
        // 获取状态信息
        sourceStatus = source.getStatus
        sinkStatus = sink.getStatus
        // check task
        checkThreadTask
        // wait thread result
        waitResult
        // shutdown
        shutdown
      }
    }
    catch {
      case e: BeeException =>
        error(s"${beeName} start fail. ", e)
    }
  }

  private def isStarted: Boolean = synchronized {
      executor != null
  }

  def shutdown = {
      isShuttingDown.set(true)
      source.shutdown
      sink.shutdown
      timer.cancel()
      executor.shutdownNow()
      executor = null
      info("source and sink thread shutdown done.")
  }

  def getOffset(): Long = {
    val offsetname = s"${conf.get("bee.offset.dir", null)}/${beeName}-offset"
    val file = new File(offsetname)
    if(!file.exists()){
      file.createNewFile()
    }
    val offsetMgr = new OffsetMgr(file)
    offsetMgr.read()
  }

  private def runSource = {
    source.setChannel(channel)
    source.skip(getOffset)
    // submit source thread
    val future: Future[ExecStatus] = executor.submit(source)
    futures(0) = future
  }

  private def runSink = {
    sink.setChannel(channel)
    // submit source thread
    val future: Future[ExecStatus] = executor.submit(sink)
    futures(1) = future
  }

  private def waitResult = {
    if(futures.isEmpty) {
      throw new BeeException("start source and sink fail .")
    }
    sourceStatus = futures(0).get
    sinkStatus = futures(1).get

  }

  def getSource: BeeSource = source

  def getChannel: BeeChannel = channel

  def getSink: BeeSink = sink


  private def checkThreadTask = {
    val task = new TimerTask {
      override def run(): Unit = {
        Array(sourceStatus, sinkStatus).foreach(x => {
          if(x == null) {
            warn("是不是一直在错着呢")
            return
          }
          val status = x.getStatus
          if(status == StatusEnum.EXCEPTIONSTOP)
            throw new BeeException(s"${x.who} exception exits!")

          if(isShuttingDown.get) {
            timer.cancel()
            info(s"${x.who} timer task cancel!")
          }
        })
        info(s"${sourceStatus.who} status: ${sourceStatus.getStatus.toString}  position: ${source.getPosition}, " +
          s"${sinkStatus.who} status: ${sinkStatus.getStatus.toString} offset: ${getOffset} ")
      }
    }
    timer.schedule(task, 1000, 5000)
    info("start check task thread.")

  }



}
