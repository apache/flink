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

package org.apache.flink.runtime.server

import java.io.{File, IOException}
import java.net.InetSocketAddress

import akka.actor._
import grizzled.slf4j.Logger
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.messages.ServerMessages._
import org.apache.flink.runtime.process.ProcessReaper
import org.apache.flink.runtime.security.SecurityUtils
import org.apache.flink.runtime.security.SecurityUtils.FlinkSecuredRunner
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.runtime.{FlinkActor, LogMessages}

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps


class ParameterServer(
    protected val flinkConfiguration: Configuration,
    protected val storeManager: StoreManager)
  extends FlinkActor
  with LogMessages // order of the mixin is important, we want first logging
  {

  override val log = Logger(getClass)

  /**
   * Run when the parameter server is started. Simply logs an informational message.
   */
  override def preStart(): Unit = {
    log.info(s"Starting ParameterServer at ${self.path.toSerializationFormat}.")
  }

  /**
   * Run post stopping
   */
  override def postStop(): Unit = {
    storeManager.shutdown()
    log.info(s"Stopping ParameterServer ${self.path.toSerializationFormat}.")

    log.debug(s"ParameterServer ${self.path} is completely stopped.")
  }

  override def handleMessage: Receive = {
    case RegisterClient(id, key, value, strategy, slack) =>
      storeManager.registerClient(id, key, value, strategy, slack, sender())

    case UpdateParameter(key, update) =>
      storeManager.update(key, update, sender())

    case PullParameter(key) =>
      try {
        sender() ! PullSuccess(storeManager.pull(key))
      } catch{
        case e: Exception =>
          sender() ! PullFailure(e.toString)
      }
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }
}

/**
 * Parameter Server companion object. Contains the entry point (main method) to run the
 * Parameter Server in a standalone fashion. Also contains various utility methods to start
 * the ParameterServer and to look up the Parameter Server actor reference.
 */
object ParameterServer {

  val LOG = Logger(classOf[ParameterServer])

  val STARTUP_FAILURE_RETURN_CODE = 1
  val RUNTIME_FAILURE_RETURN_CODE = 2

  /** Name of the ParameterServer actor */
  val PARAMETER_SERVER_NAME = "pmserver"

  /**
   * Entry point (main method) to run the ParameterServer in a standalone fashion.
   *
   * @param args The command line arguments.
   */
  def main(args: Array[String]): Unit = {
    // startup checks and logging
    EnvironmentInformation.logEnvironmentInfo(LOG.logger, "ParameterServer", args)
    EnvironmentInformation.checkJavaVersion()

    // parsing the command line arguments
    val (configuration: Configuration, listeningHost: String, listeningPort: Int) =
    try {
      parseArgs(args)
    }
    catch {
      case t: Throwable => {
        LOG.error(t.getMessage, t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
        null
      }
    }

    // we want to check that the ParameterServer hostname is in the config
    // if it is not in there, the actor system will bind to the loopback interface's
    // address and will not be reachable from anyone remote
    if (listeningHost == null) {
      val message = "Config parameter '" + ConfigConstants.PARAMETER_SERVER_IPC_ADDRESS_KEY +
        "' is missing (hostname/address to bind ParameterServer to)."
      LOG.error(message)
      System.exit(STARTUP_FAILURE_RETURN_CODE)
    }

    // address and will not be reachable from anyone remote
    if (listeningPort <= 0 || listeningPort >= 65536) {
      val message = "Config parameter '" + ConfigConstants.PARAMETER_SERVER_IPC_PORT_KEY +
        "' is invalid, it must be great than 0 and less than 65536."
      LOG.error(message)
      System.exit(STARTUP_FAILURE_RETURN_CODE)
    }

    // run the parameter server
    try {
      if (SecurityUtils.isSecurityEnabled) {
        LOG.info("Security is enabled. Starting secure ParameterServer.")
        SecurityUtils.runSecured(new FlinkSecuredRunner[Unit] {
          override def run(): Unit = {
            runParameterServer(configuration, listeningHost, listeningPort)
          }
        })
      }
      else {
        LOG.info("Security is not enabled. Starting non-authenticated ParameterServer.")
        runParameterServer(configuration, listeningHost, listeningPort)
      }
    }
    catch {
      case t: Throwable => {
        LOG.error("Failed to run ParameterServer.", t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
      }
    }
  }

  /**
   * Starts and runs the ParameterServer. First, this method starts a dedicated actor system
   * for the ParameterServer. Then it starts the ParameterServer actor itself.
   *
   * This method blocks indefinitely (or until the ParameterServer's actor system is shut down).
   *
   * @param configuration The configuration object for the ParameterServer.
   * @param listeningAddress The hostname where the ParameterServer should listen for messages.
   * @param listeningPort The port where the ParameterServer should listen for messages.
   */
  def runParameterServer(
      configuration: Configuration,
      listeningAddress: String,
      listeningPort: Int)
    : Unit = {

    LOG.info("Starting ParameterServer")

    // Bring up the Parameter Server actor system first, bind it to the given address.
    LOG.info(s"Starting ParameterServer actor system at $listeningAddress:$listeningPort.")

    val parameterServerSystem = try {
      val akkaConfig = AkkaUtils.getAkkaConfig(
        configuration,
        Some((listeningAddress, listeningPort))
      )
      if (LOG.isDebugEnabled) {
        LOG.debug("Using akka configuration\n " + akkaConfig)
      }
      AkkaUtils.createActorSystem(akkaConfig)
    }
    catch {
      case t: Throwable => {
        if (t.isInstanceOf[org.jboss.netty.channel.ChannelException]) {
          val cause = t.getCause()
          if (cause != null && t.getCause().isInstanceOf[java.net.BindException]) {
            val address = listeningAddress + ":" + listeningPort
            throw new Exception("Unable to create ParameterServer at address " + address +
              " - " + cause.getMessage(), t)
          }
        }
        throw new Exception("Could not create ParameterServer actor system", t)
      }
    }

    try {
      // bring up the ParameterServer actor
      LOG.info("Starting ParameterServer actor")
      val parameterServer = startParameterServerActor(
        configuration, parameterServerSystem, Some(PARAMETER_SERVER_NAME))

      // start a process reaper that watches the ParameterServer. If the ParameterServer
      // actor dies, the process reaper will kill the JVM process (to ensure easy
      // failure detection)
      LOG.debug("Starting ParameterServer process reaper")
      parameterServerSystem.actorOf(
        Props(
          classOf[ProcessReaper],
          parameterServer,
          LOG.logger,
          RUNTIME_FAILURE_RETURN_CODE),
        "ParameterServer_Process_Reaper")
    }
    catch {
      case t: Throwable => {
        LOG.error("Error while starting up ParameterServer", t)
        try {
          parameterServerSystem.shutdown()
        } catch {
          case tt: Throwable => LOG.warn("Could not cleanly shut down actor system", tt)
        }
        throw t
      }
    }

    // block until everything is shut down
    parameterServerSystem.awaitTermination()
  }

  /**
   * Loads the configuration, execution mode and the listening address from the provided command
   * line arguments.
   *
   * @param args command line arguments
   * @return Triple of configuration, listening address and port
   */
  def parseArgs(args: Array[String]):
                     (Configuration, String, Int) = {
    val parser = new scopt.OptionParser[ParameterServerCliOptions]("ParameterServer") {
      head("Flink ParameterServer")

      opt[String]("configDir") action { (arg, conf) => 
        conf.setConfigDir(arg)
        conf
      } text {
        "The configuration directory."
      }

      opt[String]("host").optional().action { (arg, conf) =>
        conf.setHost(arg)
        conf
      } text {
        "Network address for communication with the job manager"
      }
    }

    val config = parser.parse(args, new ParameterServerCliOptions()).getOrElse {
      throw new Exception(
        s"Invalid command line agruments: ${args.mkString(" ")}. Usage: ${parser.usage}")
    }
    
    val configDir = config.getConfigDir()
    
    if (configDir == null) {
      throw new Exception("Missing parameter '--configDir'")
    }

    LOG.info("Loading configuration from " + configDir)
    GlobalConfiguration.loadConfiguration(configDir)
    val configuration = GlobalConfiguration.getConfiguration()

    if (new File(configDir).isDirectory) {
      configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, configDir + "/..")
    }

    if (config.getHost() != null) {
      throw new Exception("Found an explicit address for ParameterServer communication " +
        "via the CLI option '--host'.\n. Don't set the '--host' option, so that the " +
        "ParameterServer uses the address configured under 'conf/flink-conf.yaml'.")
    }

    val host = configuration.getString(ConfigConstants.PARAMETER_SERVER_IPC_ADDRESS_KEY, null)
    val port = configuration.getInteger(ConfigConstants.PARAMETER_SERVER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_PARAMETER_SERVER_IPC_PORT)

    (configuration, host, port)
  }

  /**
   * Starts the ParameterServer based on the given configuration, in the given actor system.
   *
   * @param configuration The configuration for the ParameterServer
   * @param actorSystem Teh actor system running the ParameterServer
   * @param parameterServerActorName Optionally, the name of the parameter server actor. If
   *                                 none is given, it will be generated by akka.
   * @return A reference to ParameterServer
   */
  def startParameterServerActor(
      configuration: Configuration,
      actorSystem: ActorSystem,
      parameterServerActorName: Option[String])
    : ActorRef = {

    val storeManager = new StoreManager()
    val parameterServerProps = Props(
      classOf[ParameterServer],
      configuration,
      storeManager)

    val parameterServer: ActorRef = parameterServerActorName match {
      case Some(actorName) => actorSystem.actorOf(parameterServerProps, actorName)
      case None => actorSystem.actorOf(parameterServerProps)
    }

    parameterServer
  }

  // --------------------------------------------------------------------------
  //  Resolving the ParameterServer endpoint
  // --------------------------------------------------------------------------

  /**
   * Builds the akka actor path for the ParameterServer actor, given the socket address
   * where the ParameterServer's actor system runs.
   *
   * @param address The address of the ParameterServer's actor system.
   * @return The akka URL of the ParameterServer actor.
   */
  def getRemoteParameterServerAkkaURL(address: InetSocketAddress): String = {
    val hostPort = address.getAddress().getHostAddress() + ":" + address.getPort()
    s"akka.tcp://flink@$hostPort/user/$PARAMETER_SERVER_NAME"
  }

  /**
   * Builds the akka actor path for the ParameterServer actor to address the actor within
   * its own actor system.
   *
   * @return The local akka URL of the ParameterServer actor.
   */
  def getLocalParameterServerAkkaURL: String = {
    "akka://flink/user/" + PARAMETER_SERVER_NAME
  }

  def getParameterServerRemoteReferenceFuture(
      address: InetSocketAddress,
      system: ActorSystem,
      timeout: FiniteDuration)
    : Future[ActorRef] = {

    AkkaUtils.getReference(getRemoteParameterServerAkkaURL(address), system, timeout)
  }

  /**
   * Resolves the ParameterServer actor reference in a blocking fashion.
   *
   * @param parameterServerUrl The akka URL of the ParameterServer.
   * @param system The local actor system that should perform the lookup.
   * @param timeout The maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the ParameterServer
   */
  @throws(classOf[IOException])
  def getParameterServerRemoteReference(
      parameterServerUrl: String,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {

    try {
      val future = AkkaUtils.getReference(parameterServerUrl, system, timeout)
      Await.result(future, timeout)
    }
    catch {
      case e @ (_ : ActorNotFound | _ : TimeoutException) =>
        throw new IOException(
          s"ParameterServer at $parameterServerUrl not reachable. " +
            s"Please make sure that the ParameterServer is running and its port is reachable.", e)

      case e: IOException =>
        throw new IOException("Could not connect to ParameterServer at " + parameterServerUrl, e)
    }
  }

  /**
   * Resolves the ParameterServer actor reference in a blocking fashion.
   *
   * @param address The socket address of the ParameterServer's actor system.
   * @param system The local actor system that should perform the lookup.
   * @param timeout The maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the ParameterServer
   */
  @throws(classOf[IOException])
  def getParameterServerRemoteReference(
      address: InetSocketAddress,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {

    val pmAddress = getRemoteParameterServerAkkaURL(address)
    getParameterServerRemoteReference(pmAddress, system, timeout)
  }

  /**
   * Resolves the ParameterServer actor reference in a blocking fashion.
   *
   * @param address The socket address of the ParameterServer's actor system.
   * @param system The local actor system that should perform the lookup.
   * @param config The config describing the maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the ParameterServer
   */
  @throws(classOf[IOException])
  def getParameterServerRemoteReference(
      address: InetSocketAddress,
      system: ActorSystem,
      config: Configuration)
    : ActorRef = {

    val timeout = AkkaUtils.getLookupTimeout(config)
    getParameterServerRemoteReference(address, system, timeout)
  }
}
