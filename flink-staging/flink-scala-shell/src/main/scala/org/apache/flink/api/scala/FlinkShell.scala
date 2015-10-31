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

package org.apache.flink.api.scala

import java.io.{StringWriter, BufferedReader}

import org.apache.flink.api.common.ExecutionMode

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster

import scala.tools.nsc.Settings

import scala.tools.nsc.interpreter._


object FlinkShell {

  object ExecutionMode extends Enumeration {
    val UNDEFINED, LOCAL, REMOTE = Value
  }

  var bufferedReader: Option[BufferedReader] = None

  def main(args: Array[String]) {

    // scopt, command line arguments
    case class Config(
        port: Int = -1,
        host: String = "none",
        externalJars: Option[Array[String]] = None,
        flinkShellExecutionMode: ExecutionMode.Value = ExecutionMode.UNDEFINED)

    val parser = new scopt.OptionParser[Config]("start-scala-shell.sh") {
      head ("Flink Scala Shell")

      cmd("local") action {
        (_, c) => c.copy(host = "none", port = -1, flinkShellExecutionMode = ExecutionMode.LOCAL)
      } text("starts Flink scala shell with a local Flink cluster\n") children(
        opt[(String)] ("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
          } text("specifies additional jars to be used in Flink\n")
        )

      cmd("remote") action { (_, c) =>
        c.copy(flinkShellExecutionMode = ExecutionMode.REMOTE)
      } text("starts Flink scala shell connecting to a remote cluster\n") children(
        arg[String]("<host>") action { (h, c) =>
          c.copy(host = h) }
          text("remote host name as string"),
        arg[Int]("<port>") action { (p, c) =>
          c.copy(port = p) }
          text("remote port as integer\n"),
        opt[(String)]("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
          } text("specifies additional jars to be used in Flink")
      )
      help("help") abbr("h") text("prints this usage text\n")
    }

    // parse arguments
    parser.parse (args, Config()) match {
      case Some(config) =>
        startShell(config.host,
          config.port,
          config.flinkShellExecutionMode,
          config.externalJars)

      case _ => System.out.println("Could not parse program arguments")
    }
  }


  def startShell(
      userHost: String,
      userPort: Int,
      executionMode: ExecutionMode.Value,
      externalJars: Option[Array[String]] = None): Unit ={
    
    System.out.println("Starting Flink Shell:")

    // either port or userhost not specified by user, create new minicluster
    val (host: String, port: Int, cluster: Option[LocalFlinkMiniCluster]) =
      executionMode match {
        case ExecutionMode.LOCAL =>
          val config = new Configuration()
          config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 0)
          val miniCluster = new LocalFlinkMiniCluster(config, false)
          miniCluster.start()
          val port = miniCluster.getLeaderRPCPort
          System.out.println(s"\nStarting local Flink cluster (host: localhost, port: $port).\n")
          ("localhost", port, Some(miniCluster))

        case ExecutionMode.REMOTE =>
          if (userHost == "none" || userPort == -1) {
            System.out.println("Error: <host> or <port> not specified!")
            return
          } else {
            System.out.println(
              s"\nConnecting to Flink cluster (host: $userHost, port: $userPort).\n")
            (userHost, userPort, None)
          }

        case ExecutionMode.UNDEFINED =>
          System.out.println("Error: please specify execution mode:")
          System.out.println("[local | remote <host> <port>]")
          return
      }

    var repl: Option[FlinkILoop] = None

    try {
      // custom shell
      repl = Some(
        bufferedReader match {

          case Some(br) =>
            val out = new StringWriter()
            new FlinkILoop(host, port, externalJars, bufferedReader, new JPrintWriter(out))

          case None =>
            new FlinkILoop(host, port, externalJars)
        })

      val settings = new Settings()

      settings.usejavacp.value = true
      settings.Yreplsync.value = true

      // start scala interpreter shell
      repl.foreach(_.process(settings))
    } finally {
      repl.foreach(_.closeInterpreter())
      cluster.foreach(_.stop())
    }

    System.out.println(" good bye ..")
  }
}
