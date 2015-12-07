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
import java.util.Properties

import org.apache.flink.client.{CliFrontend, ClientUtils, FlinkYarnSessionCli}
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.runtime.minicluster.{FlinkMiniCluster, LocalFlinkMiniCluster}
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster
import org.apache.hadoop.fs.Path

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
    yarnConfig: Option[YarnConfig] = None
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
      } text("Starts Flink scala shell with a local Flink cluster") children(
        opt[(String)] ("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
          } text("Specifies additional jars to be used in Flink")
        )

      cmd("remote") action { (_, c) =>
        c.copy(executionMode = ExecutionMode.REMOTE)
      } text("Starts Flink scala shell connecting to a remote cluster") children(
        arg[String]("<host>") action { (h, c) =>
          c.copy(host = Some(h)) }
          text("Remote host name as string"),
        arg[Int]("<port>") action { (p, c) =>
          c.copy(port = Some(p)) }
          text("Remote port as integer\n"),
        opt[(String)]("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
        } text ("Specifies additional jars to be used in Flink")
      )

      cmd("yarn") action {
        (_, c) => c.copy(executionMode = ExecutionMode.YARN, yarnConfig = None)
      } text ("Starts Flink scala shell connecting to a yarn cluster") children(
        opt[Int]("container") abbr ("n") valueName ("arg") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(containers = Some(x))))
        } text ("Number of YARN container to allocate (= Number of TaskManagers)"),
        opt[Int]("jobManagerMemory") abbr ("jm") valueName ("arg") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(jobManagerMemory = Some(x))))
        } text ("Memory for JobManager container [in MB]"),
        opt[String]("name") abbr ("nm") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(name = Some(x))))
        } text ("Set a custom name for the application on YARN"),
        opt[String]("queue") abbr ("qu") valueName ("<arg>") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(queue = Some(x))))
        } text ("Specifies YARN queue"),
        opt[Int]("slots") abbr ("s") valueName ("<arg>") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(slots = Some(x))))
        } text ("Number of slots per TaskManager"),
        opt[Int]("taskManagerMemory") abbr ("tm") valueName ("<arg>") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(taskManagerMemory = Some(x))))
        } text ("Memory per TaskManager container [in MB]"),
        opt[(String)] ("addclasspath") abbr("a") valueName("<path/to/jar>") action {
          case (x, c) =>
            val xArray = x.split(":")
            c.copy(externalJars = Option(xArray))
        } text("Specifies additional jars to be used in Flink")
      )

      help("help") abbr ("h") text ("Prints this usage text")
    }

    // parse arguments
    parser.parse(args, Config()) match {
      case Some(config) => startShell(config)
      case _ => println("Could not parse program arguments")
    }
  }

  def fetchConnectionInfo(
    config: Config
  ): (String, Int, Option[Either[FlinkMiniCluster, AbstractFlinkYarnCluster]]) = {
    config.executionMode match {
      case ExecutionMode.LOCAL => // Local mode
        val config = new Configuration()
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
        case Some(Right(yarnCluster)) => yarnCluster.shutdown(false)
        case _ =>
      }
    }

    println(" good bye ..")
  }

  def deployNewYarnCluster(yarnConfig: YarnConfig) = {
    val yarnClient = FlinkYarnSessionCli.getFlinkYarnClient

    // use flink-dist.jar for scala shell
    val jarPath = new Path("file://" +
      s"${yarnClient.getClass.getProtectionDomain.getCodeSource.getLocation.getPath}")
    yarnClient.setLocalJarPath(jarPath)

    // load configuration
    val confDirPath = CliFrontend.getConfigurationDirectoryFromEnv
    val flinkConfiguration = GlobalConfiguration.getConfiguration
    val confFile = new File(confDirPath + File.separator + "flink-conf.yaml")
    val confPath = new Path(confFile.getAbsolutePath)
    GlobalConfiguration.loadConfiguration(confDirPath)
    yarnClient.setFlinkConfigurationObject(flinkConfiguration)
    yarnClient.setConfigurationDirectory(confDirPath)
    yarnClient.setConfigurationFilePath(confPath)

    // number of task managers is required.
    yarnConfig.containers match {
      case Some(containers) => yarnClient.setTaskManagerCount(containers)
      case None =>
        throw new IllegalArgumentException("Number of taskmanagers must be specified.")
    }

    // set configuration from user input
    yarnConfig.jobManagerMemory.foreach(yarnClient.setJobManagerMemory)
    yarnConfig.name.foreach(yarnClient.setName)
    yarnConfig.queue.foreach(yarnClient.setQueue)
    yarnConfig.slots.foreach(yarnClient.setTaskManagerSlots)
    yarnConfig.taskManagerMemory.foreach(yarnClient.setTaskManagerMemory)

    // deploy
    val cluster = yarnClient.deploy()
    val address = cluster.getJobManagerAddress.getAddress.getHostAddress
    val port = cluster.getJobManagerAddress.getPort
    cluster.connectToCluster()

    (address, port, Some(Right(cluster)))
  }

  def fetchDeployedYarnClusterInfo() = {
    // load configuration
    val globalConfig = GlobalConfiguration.getConfiguration
    val propertiesLocation = CliFrontend.getYarnPropertiesLocation(globalConfig)
    val propertiesFile = new File(propertiesLocation)

    // read properties
    val properties = if (propertiesFile.exists()) {
      println("Found YARN properties file " + propertiesFile.getAbsolutePath)
      val properties = new Properties()
      val inputStream = new FileInputStream(propertiesFile)

      try {
        properties.load(inputStream)
      } finally {
        inputStream.close()
      }

      properties
    } else {
      throw new IllegalArgumentException("Scala Shell cannot fetch YARN properties.")
    }

    val addressInStr = properties.getProperty(CliFrontend.YARN_PROPERTIES_JOBMANAGER_KEY)
    val address = ClientUtils.parseHostPortAddress(addressInStr)

    (address.getHostString, address.getPort, None)
  }

  def ensureYarnConfig(config: Config) = config.yarnConfig match {
    case Some(yarnConfig) => yarnConfig
    case None => YarnConfig()
  }
}
