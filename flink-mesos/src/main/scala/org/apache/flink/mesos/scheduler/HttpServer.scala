/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mesos.scheduler

import java.nio.file.{Files, Paths}

import unfiltered.request.{Path, Seg}
import unfiltered.response.ResponseString

import scala.io.Source

class HttpServer(conf: Conf) {
  val confDir: String = conf.confDir

  val consoleLog4jConfiguration = ResponseString(
    """
      |log4j.rootLogger=INFO, stdout
      |
      |log4j.appender.stdout=org.apache.log4j.ConsoleAppender
      |log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
      |log4j.appender.stdout.layout.ConversionPattern=%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n
      |
      |log4j.logger.org.jboss.netty.channel.DefaultChannelPipeline=ERROR, stdout
      |
    """.stripMargin)

  // create a plan to serve flink-config.yaml and log4j.properties via http
  val configServerPlan = unfiltered.filter.Planify {
    case Path(Seg("log4j.properties" :: Nil)) =>
      // get the log4j configuration and send it
      val confDirLog4JPropsFile = Paths.get(confDir, "log4j.properties")
      sys.env.get("log4j.configuration") match {
        case Some(filePath: String) =>
          new ResponseString(Source.fromFile(filePath).mkString)
        case None if Files.exists(confDirLog4JPropsFile) =>
          new ResponseString(Source.fromFile(confDirLog4JPropsFile.toString).mkString)
        case None =>
          consoleLog4jConfiguration
      }
  }

  // serve using an embedded jetty server running via any available local port
  val svr = {
    val _svr = if (conf.port == -1) {
      unfiltered.jetty.Server.anylocal
    } else {
      unfiltered.jetty.Server.local(conf.port)
    }
    _svr.plan(configServerPlan)
  }

  val port = svr.ports.last

  def start(): Unit = {
    svr.run()
  }

  val host: String = {
    if (conf.host == "") {
      svr.portBindings.map(_.connector.getHost).head
    } else {
      conf.host
    }
  }

}
