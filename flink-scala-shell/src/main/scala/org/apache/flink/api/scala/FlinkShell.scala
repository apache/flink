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

import org.apache.flink.annotation.Internal
import org.apache.flink.client.cli.{CliFrontend, CliFrontendParser}
import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader
import org.apache.flink.client.program.{ClusterClient, MiniClusterClient}
import org.apache.flink.configuration.{ConfigConstants, Configuration, DeploymentOptions, GlobalConfiguration, JobManagerOptions, RestOptions, TaskManagerOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}

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
    jobManagerMemory: Option[String] = None,
    name: Option[String] = None,
    queue: Option[String] = None,
    slots: Option[Int] = None,
    taskManagerMemory: Option[String] = None
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
        opt[String]("jobManagerMemory") abbr ("jm") valueName ("arg") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(jobManagerMemory = Some(x))))
        } text "Memory for JobManager container",
        opt[String]("name") abbr ("nm") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(name = Some(x))))
        } text "Set a custom name for the application on YARN",
        opt[String]("queue") abbr ("qu") valueName ("<arg>") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(queue = Some(x))))
        } text "Specifies YARN queue",
        opt[Int]("slots") abbr ("s") valueName ("<arg>") action {
          (x, c) => c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(slots = Some(x))))
        } text "Number of slots per TaskManager",
        opt[String]("taskManagerMemory") abbr ("tm") valueName ("<arg>") action {
          (x, c) =>
            c.copy(yarnConfig = Some(ensureYarnConfig(c).copy(taskManagerMemory = Some(x))))
        } text "Memory per TaskManager container",
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


  @Internal def ensureYarnConfig(config: Config) = config.yarnConfig match {
    case Some(yarnConfig) => yarnConfig
    case None => YarnConfig()
  }

  private def getConfigDir(config: Config) = {
    config.configDir.getOrElse(CliFrontend.getConfigurationDirectoryFromEnv)
  }

  private def getGlobalConfig(config: Config) = {
    val confDirPath = getConfigDir(config)
    val configDirectory = new File(confDirPath)
    GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath)
  }

  def startShell(config: Config): Unit = {
    println("Starting Flink Shell:")

    val flinkConfig = getGlobalConfig(config)

    val (repl, clusterClient) = try {
      val (effectiveConfig, clusterClient) = fetchConnectionInfo(config, flinkConfig)

      val host = effectiveConfig.getString(JobManagerOptions.ADDRESS)
      val port = effectiveConfig.getInteger(JobManagerOptions.PORT)
      println(s"\nConnecting to Flink cluster (host: $host, port: $port).\n")

      val repl = bufferedReader match {
        case Some(reader) =>
          val out = new StringWriter()
          new FlinkILoop(effectiveConfig, config.externalJars, reader, new JPrintWriter(out))
        case None =>
          new FlinkILoop(effectiveConfig, config.externalJars)
      }

      (repl, clusterClient)
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
      clusterClient match {
        case Some(clusterClient) =>
          clusterClient.shutDownCluster()
          clusterClient.close()
        case _ =>
      }
    }

    println(" good bye ..")
  }

  @Internal def fetchConnectionInfo(
      config: Config,
      flinkConfig: Configuration): (Configuration, Option[ClusterClient[_]]) = {

    config.executionMode match {
      case ExecutionMode.LOCAL => createLocalClusterAndConfig(flinkConfig)
      case ExecutionMode.REMOTE => createRemoteConfig(config, flinkConfig)
      case ExecutionMode.YARN => createYarnClusterIfNeededAndGetConfig(config, flinkConfig)
      case ExecutionMode.UNDEFINED => // Wrong input
        throw new IllegalArgumentException("please specify execution mode:\n" +
          "[local | remote <host> <port> | yarn]")
    }
  }

  private def createYarnClusterIfNeededAndGetConfig(config: Config, flinkConfig: Configuration) = {
    flinkConfig.setBoolean(DeploymentOptions.ATTACHED, true)

    val (clusterConfig, clusterClient) = config.yarnConfig match {
      case Some(_) => deployNewYarnCluster(config, flinkConfig)
      case None => (flinkConfig, None)
    }

    val (effectiveConfig, _) = clusterClient match {
      case Some(_) => fetchDeployedYarnClusterInfo(config, clusterConfig, "yarn-cluster")
      case None => fetchDeployedYarnClusterInfo(config, clusterConfig, "default")
    }

    println("Configuration: " + effectiveConfig)

    (effectiveConfig, clusterClient)
  }

  private def deployNewYarnCluster(config: Config, flinkConfig: Configuration) = {
    val effectiveConfig = new Configuration(flinkConfig)
    val args = parseArgList(config, "yarn-cluster")

    val configurationDirectory = getConfigDir(config)

    val frontend = new CliFrontend(
      effectiveConfig,
      CliFrontend.loadCustomCommandLines(effectiveConfig, configurationDirectory))

    val commandOptions = CliFrontendParser.getRunCommandOptions
    val commandLineOptions = CliFrontendParser.mergeOptions(commandOptions,
      frontend.getCustomCommandLineOptions)
    val commandLine = CliFrontendParser.parse(commandLineOptions, args, true)

    val customCLI = frontend.validateAndGetActiveCommandLine(commandLine)
    effectiveConfig.addAll(customCLI.toConfiguration(commandLine))

    val serviceLoader = new DefaultClusterClientServiceLoader
    val clientFactory = serviceLoader.getClusterClientFactory(effectiveConfig)
    val clusterDescriptor = clientFactory.createClusterDescriptor(effectiveConfig)
    val clusterSpecification = clientFactory.getClusterSpecification(effectiveConfig)

    val clusterClient = try {
      clusterDescriptor
        .deploySessionCluster(clusterSpecification)
        .getClusterClient
    } finally {
      effectiveConfig.set(DeploymentOptions.TARGET, "yarn-session")
      clusterDescriptor.close()
    }

    (effectiveConfig, Some(clusterClient))
  }

  private def fetchDeployedYarnClusterInfo(
      config: Config,
      flinkConfig: Configuration,
      mode: String) = {

    val effectiveConfig = new Configuration(flinkConfig)
    val args = parseArgList(config, mode)

    val configurationDirectory = getConfigDir(config)

    val frontend = new CliFrontend(
      effectiveConfig,
      CliFrontend.loadCustomCommandLines(effectiveConfig, configurationDirectory))

    val commandOptions = CliFrontendParser.getRunCommandOptions
    val commandLineOptions = CliFrontendParser.mergeOptions(commandOptions,
      frontend.getCustomCommandLineOptions)
    val commandLine = CliFrontendParser.parse(commandLineOptions, args, true)

    val customCLI = frontend.validateAndGetActiveCommandLine(commandLine)
    effectiveConfig.addAll(customCLI.toConfiguration(commandLine))

    (effectiveConfig, None)
  }

  def parseArgList(config: Config, mode: String): Array[String] = {
    val args = if (mode == "default") {
      ArrayBuffer[String]()
    } else {
      ArrayBuffer[String]("-m", mode)
    }

    config.yarnConfig match {
      case Some(yarnConfig) =>
        yarnConfig.jobManagerMemory.foreach((jmMem) => args ++= Seq("-yjm", jmMem.toString))
        yarnConfig.taskManagerMemory.foreach((tmMem) => args ++= Seq("-ytm", tmMem.toString))
        yarnConfig.name.foreach((name) => args ++= Seq("-ynm", name.toString))
        yarnConfig.queue.foreach((queue) => args ++= Seq("-yqu", queue.toString))
        yarnConfig.slots.foreach((slots) => args ++= Seq("-ys", slots.toString))
        args.toArray
      case None => args.toArray
    }
  }

  private def createRemoteConfig(
      config: Config,
      flinkConfig: Configuration): (Configuration, None.type) = {

    if (config.host.isEmpty || config.port.isEmpty) {
      throw new IllegalArgumentException("<host> or <port> is not specified!")
    }

    val effectiveConfig = new Configuration(flinkConfig)
    setJobManagerInfoToConfig(effectiveConfig, config.host.get, config.port.get)
    effectiveConfig.set(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    effectiveConfig.setBoolean(DeploymentOptions.ATTACHED, true)

    (effectiveConfig, None)
  }

  private def createLocalClusterAndConfig(flinkConfig: Configuration) = {
    val config = new Configuration(flinkConfig)
    config.setInteger(JobManagerOptions.PORT, 0)

    val cluster = createLocalCluster(config)
    val port = cluster.getRestAddress.get.getPort

    setJobManagerInfoToConfig(config, "localhost", port)
    config.set(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    config.setBoolean(DeploymentOptions.ATTACHED, true)

    println(s"\nStarting local Flink cluster (host: localhost, port: ${port}).\n")

    val clusterClient = new MiniClusterClient(config, cluster)
    (config, Some(clusterClient))
  }

  private def createLocalCluster(flinkConfig: Configuration) = {

    val numTaskManagers = flinkConfig.getInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
      ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)
    val numSlotsPerTaskManager = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS)

    val miniClusterConfig = new MiniClusterConfiguration.Builder()
      .setConfiguration(flinkConfig)
      .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
      .setNumTaskManagers(numTaskManagers)
      .build()

    val cluster = new MiniCluster(miniClusterConfig)
    cluster.start()
    cluster
  }

  private def setJobManagerInfoToConfig(
      config: Configuration,
      host: String, port: Integer): Unit = {

    config.setString(JobManagerOptions.ADDRESS, host)
    config.setInteger(JobManagerOptions.PORT, port)

    config.setString(RestOptions.ADDRESS, host)
    config.setInteger(RestOptions.PORT, port)
  }
}
