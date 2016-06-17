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

import java.io._

import org.apache.commons.cli.CommandLine
import org.apache.flink.client.cli.CliFrontendParser
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.client.CliFrontend
import org.apache.flink.configuration.{ConfigConstants, GlobalConfiguration}
import org.apache.flink.runtime.minicluster.{FlinkMiniCluster, LocalFlinkMiniCluster}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter._

object FlinkShell {

  object ExecutionMode extends Enumeration {
    val UNDEFINED, LOCAL, REMOTE, YARN = Value
  }

  /** Configuration object */
  case class Config(
    host: Option[String] = None,
    port: Option[Int] = None,
    externalJars: Option[Array[String]] = None,
    executionMode: ExecutionMode.Value = ExecutionMode.UNDEFINED,
    yarnConfig: Option[YarnConfig] = None,
    configDir: Option[String] = None
  )

  /** YARN configuration object */
  case class YarnConfig(
    containers: Option[Int] = None,
    jobManagerMemory: Option[Int] = None,
    name: Option[String] = None,
    queue: Option[String] = None,
    slots: Option[Int] = None,
    taskManagerMemory: Option[Int] = None
  )

  /** Buffered reader to substitute input in test */
  var bufferedReader: Option[BufferedReader] = None

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("start-scala-shell.sh") {
      head("Flink Scala Shell")

      cmd("local") action {
        (_, c) => c.copy(executionMode = ExecutionMode.LOCAL)
      } text "Starts Flink scala shell with a local Flink cluster" children(
        opt[(String)] ("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
          } text "Specifies additional jars to be used in Flink"
        )

      cmd("remote") action { (_, c) =>
        c.copy(executionMode = ExecutionMode.REMOTE)
      } text "Starts Flink scala shell connecting to a remote cluster" children(
        arg[String]("<host>") action { (h, c) =>
          c.copy(host = Some(h)) }
          text "Remote host name as string",
        arg[Int]("<port>") action { (p, c) =>
          c.copy(port = Some(p)) }
          text "Remote port as integer\n",
        opt[String]("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
        } text "Specifies additional jars to be used in Flink"
      )

      cmd("yarn") action {
        (_, c) => c.copy(executionMode = ExecutionMode.YARN, yarnConfig = None)
      } text "Starts Flink scala shell connecting to a yarn cluster" children(
        opt[Int]("container") abbr ("n") valueName ("arg") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(containers = Some(x))))
        } text "Number of YARN container to allocate (= Number of TaskManagers)",
        opt[Int]("jobManagerMemory") abbr ("jm") valueName ("arg") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(jobManagerMemory = Some(x))))
        } text "Memory for JobManager container [in MB]",
        opt[String]("name") abbr ("nm") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(name = Some(x))))
        } text "Set a custom name for the application on YARN",
        opt[String]("queue") abbr ("qu") valueName ("<arg>") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(queue = Some(x))))
        } text "Specifies YARN queue",
        opt[Int]("slots") abbr ("s") valueName ("<arg>") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(slots = Some(x))))
        } text "Number of slots per TaskManager",
        opt[Int]("taskManagerMemory") abbr ("tm") valueName ("<arg>") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(taskManagerMemory = Some(x))))
        } text "Memory per TaskManager container [in MB]",
        opt[(String)] ("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
        } text "Specifies additional jars to be used in Flink"
      )

      opt[String]("configDir").optional().action {
        (arg, conf) => conf.copy(configDir = Option(arg))
      } text {
        "The configuration directory."
      }

      help("help") abbr ("h") text "Prints this usage text"
    }

    // parse arguments
    parser.parse(args, Config()) match {
      case Some(config) => startShell(config)
      case _ => println("Could not parse program arguments")
    }
  }

  def fetchConnectionInfo(
    config: Config
  ): (String, Int, Option[Either[FlinkMiniCluster, ClusterClient]]) = {
    config.executionMode match {
      case ExecutionMode.LOCAL => // Local mode
        val config = GlobalConfiguration.getConfiguration()
        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 0)

        val miniCluster = new LocalFlinkMiniCluster(config, false)
        miniCluster.start()

        println("\nStarting local Flink cluster (host: localhost, " +
          s"port: ${miniCluster.getLeaderRPCPort}).\n")
        ("localhost", miniCluster.getLeaderRPCPort, Some(Left(miniCluster)))

      case ExecutionMode.REMOTE => // Remote mode
        if (config.host.isEmpty || config.port.isEmpty) {
          throw new IllegalArgumentException("<host> or <port> is not specified!")
        }
        (config.host.get, config.port.get, None)

      case ExecutionMode.YARN => // YARN mode
        config.yarnConfig match {
          case Some(yarnConfig) => // if there is information for new cluster
            deployNewYarnCluster(yarnConfig)
          case None => // there is no information for new cluster. Then we use yarn properties.
            fetchDeployedYarnClusterInfo()
        }

      case ExecutionMode.UNDEFINED => // Wrong input
        throw new IllegalArgumentException("please specify execution mode:\n" +
          "[local | remote <host> <port> | yarn]")
    }
  }

  def startShell(config: Config): Unit = {
    println("Starting Flink Shell:")

    // load global configuration
    val confDirPath = config.configDir match {
      case Some(confDir) => confDir
      case None => CliFrontend.getConfigurationDirectoryFromEnv
    }

    val configDirectory = new File(confDirPath)
    GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath)

    val (repl, cluster) = try {
      val (host, port, cluster) = fetchConnectionInfo(config)
      val conf = cluster match {
        case Some(Left(miniCluster)) => miniCluster.userConfiguration
        case Some(Right(yarnCluster)) => yarnCluster.getFlinkConfiguration
        case None => GlobalConfiguration.getConfiguration
      }

      println(s"\nConnecting to Flink cluster (host: $host, port: $port).\n")
      val repl = bufferedReader match {
        case Some(reader) =>
          val out = new StringWriter()
          new FlinkILoop(host, port, conf, config.externalJars, reader, new JPrintWriter(out))
        case None =>
          new FlinkILoop(host, port, conf, config.externalJars)
      }

      (repl, cluster)
    } catch {
      case e: IllegalArgumentException =>
        println(s"Error: ${e.getMessage}")
        sys.exit()
    }

    val settings = new Settings()
    settings.usejavacp.value = true
    settings.Yreplsync.value = true

    try {
      repl.process(settings)
    } finally {
      repl.closeInterpreter()
      cluster match {
        case Some(Left(miniCluster)) => miniCluster.stop()
        case Some(Right(yarnCluster)) => yarnCluster.shutdown()
        case _ =>
      }
    }

    println(" good bye ..")
  }

  def deployNewYarnCluster(yarnConfig: YarnConfig) = {

    val args = ArrayBuffer[String](
      "-m", "yarn-cluster"
    )

    // number of task managers is required.
    yarnConfig.containers match {
      case Some(containers) => args ++= Seq("-yn", containers.toString)
      case None =>
        throw new IllegalArgumentException("Number of taskmanagers must be specified.")
    }

    // set configuration from user input
    yarnConfig.jobManagerMemory.foreach((jmMem) => args ++= Seq("-yjm", jmMem.toString))
    yarnConfig.slots.foreach((tmMem) => args ++= Seq("-ytm", tmMem.toString))
    yarnConfig.name.foreach((name) => args ++= Seq("-ynm", name.toString))
    yarnConfig.queue.foreach((queue) => args ++= Seq("-yqu", queue.toString))
    yarnConfig.slots.foreach((slots) => args ++= Seq("-ys", slots.toString))


    val options = CliFrontendParser.parseRunCommand(args.toArray)
    val frontend = new CliFrontend()
    val config = frontend.getConfiguration
    val customCLI = frontend.getActiveCustomCommandLine(options.getCommandLine)

    val cluster = customCLI.createCluster("Flink Scala Shell", options.getCommandLine, config)

    val address = cluster.getJobManagerAddress.getAddress.getHostAddress
    val port = cluster.getJobManagerAddress.getPort

    (address, port, Some(Right(cluster)))
  }

  def fetchDeployedYarnClusterInfo() = {


    val args = ArrayBuffer[String](
      "-m", "yarn-cluster"
    )

    val options = CliFrontendParser.parseRunCommand(args.toArray)
    val frontend = new CliFrontend()
    val config = frontend.getConfiguration
    val customCLI = frontend.getActiveCustomCommandLine(options.getCommandLine)

    val cluster = customCLI.retrieveCluster(options.getCommandLine, config)

    if (cluster == null) {
      throw new RuntimeException("Yarn Cluster could not be retrieved.")
    }

    val jobManager = cluster.getJobManagerAddress

    (jobManager.getHostString, jobManager.getPort, None)
  }

  def ensureYarnConfig(config: Config) = config.yarnConfig match {
    case Some(yarnConfig) => yarnConfig
    case None => YarnConfig()
  }
}
