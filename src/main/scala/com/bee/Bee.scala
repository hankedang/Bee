package com.bee

import com.bee.common.util.Logging
import com.bee.core.BeeServer
import com.sun.jersey.spi.container.servlet.ServletContainer
import org.apache.log4j.PropertyConfigurator
import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.{Context, ServletHolder}

/**
  * Created by han.kedang on 16/8/30.
  */
object Bee extends Logging {


  def startRestApi = {
    try {
      val server = new Server(9099)
      val servlet = new ServletHolder(new ServletContainer().getClass)
      servlet.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig")
      servlet.setInitParameter("com.sun.jersey.config.property.packages", "com.bee.api.rest")

      val context = new Context
      context.setContextPath("/")
      context.addServlet(servlet, "/*")
      server.setHandler(context)

      server.start
    }
    catch {
      case e: Throwable =>
        error(e)
        System.exit(3)
    }

  }

  def main(args: Array[String]): Unit = {

    PropertyConfigurator.configure(System.getenv("BEE_HOME") + "/conf/log4j.properties")

    try {

      val ret = BeeServer.startup

      // launch on rest api
      startRestApi

      if(ret != 0) {
        error(s"start BeeServer fail! ret: ${ret}")
        System.exit(1)
      }
    }
    catch {
      case e: Throwable =>
        error(e)
        System.exit(2)
    }
  }

}
