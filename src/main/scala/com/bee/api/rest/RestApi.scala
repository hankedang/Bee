package com.bee.api.rest

import javax.ws.rs.{GET, Path, PathParam, Produces}

import com.bee.core.BeeServer

/**
  * Created by han.kedang on 16/9/14.
  */


@Path("bee")
class RestApi {

  @GET
  @Path("start/{name}")
  @Produces(Array("text/plain"))
  def startSchdule(@PathParam("name") name: String) = {
    val ret = BeeServer.startSchedule(name)
    if (ret != 0)
      s"start ${name} scheduler fail, ret: ${ret}"
    else
      "Ok"
  }

  @GET
  @Path("start")
  @Produces(Array("text/plain"))
  def startAll = {
    val ret = BeeServer.startup
    if (ret != 0)
      s"start all scheduler fail, ret: ${ret}"
    else
      "Ok"
  }

  @GET
  @Path("stop/{name}")
  @Produces(Array("text/plain"))
  def stopSchdule(@PathParam("name") name: String) = {
    BeeServer.shutdown(name)
    s"send shutdown ${name} scheduler command"
  }

  @GET
  @Path("stop")
  @Produces(Array("text/plain"))
  def stopAll = {
    BeeServer.shutdownAll
    s"send shutdown all scheduler command"
  }

  @GET
  @Path("query")
  @Produces(Array("text/plain"))
  def queryAll = {
    BeeServer.queryStatus.toString
  }




}
