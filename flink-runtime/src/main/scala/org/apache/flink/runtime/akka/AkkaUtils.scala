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

package org.apache.flink.runtime.akka

import java.net.InetAddress
import java.util.concurrent.{TimeUnit, Callable}

import akka.actor._
import akka.pattern.{ask => akkaAsk}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.jboss.netty.logging.{Slf4JLoggerFactory, InternalLoggerFactory}
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * This class contains utility functions for akka. It contains methods to start an actor system with
 * a given akka configuration. Furthermore, the akka configuration used for starting the different
 * actor systems resides in this class.
 */
object AkkaUtils {
  val LOG = LoggerFactory.getLogger(AkkaUtils.getClass)

  val INF_TIMEOUT = 21474835 seconds

  var globalExecutionContext: ExecutionContext = ExecutionContext.global

  /**
   * Creates a local actor system without remoting.
   *
   * @param configuration instance containing the user provided configuration values
   * @return The created actor system
   */
  def createLocalActorSystem(configuration: Configuration): ActorSystem = {
    val akkaConfig = getAkkaConfig(configuration, None)
    createActorSystem(akkaConfig)
  }

  /**
   * Creates an actor system. If a listening address is specified, then the actor system will listen
   * on that address for messages from a remote actor system. If not, then a local actor system
   * will be instantiated.
   *
   * @param configuration instance containing the user provided configuration values
   * @param listeningAddress an optional tuple containing a hostname and a port to bind to. If the
   *                         parameter is None, then a local actor system will be created.
   * @return created actor system
   */
  def createActorSystem(configuration: Configuration,
                        listeningAddress: Option[(String, Int)]): ActorSystem = {
    val akkaConfig = getAkkaConfig(configuration, listeningAddress)
    createActorSystem(akkaConfig)
  }

  /**
   * Creates an actor system with the given akka config.
   *
   * @param akkaConfig configuration for the actor system
   * @return created actor system
   */
  def createActorSystem(akkaConfig: Config): ActorSystem = {
    // Initialize slf4j as logger of Akka's Netty instead of java.util.logging (FLINK-1650)
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)
    ActorSystem.create("flink", akkaConfig)
  }

  /**
   * Creates an actor system with the default config and listening on a random port of the
   * localhost.
   *
   * @return default actor system listening on a random port of the localhost
   */
  def createDefaultActorSystem(): ActorSystem = {
    createActorSystem(getDefaultAkkaConfig)
  }

  /**
   * Creates an akka config with the provided configuration values. If the listening address is
   * specified, then the actor system will listen on the respective address.
   *
   * @param configuration instance containing the user provided configuration values
   * @param listeningAddress optional tuple of hostname and port to listen on. If None is given,
   *                         then an Akka config for local actor system will be returned
   * @return Akka config
   */
  def getAkkaConfig(configuration: Configuration,
                    listeningAddress: Option[(String, Int)]): Config = {
    val defaultConfig = getBasicAkkaConfig(configuration)

    listeningAddress match {

      case Some((hostname, port)) =>
        val ipAddress = "\"" + InetAddress.getByName(hostname).getHostAddress() + "\""
        val remoteConfig = getRemoteAkkaConfig(configuration, ipAddress, port)
        remoteConfig.withFallback(defaultConfig)

      case None =>
        defaultConfig
    }
  }

  /**
   * Creates the default akka configuration which listens on a random port on the local machine.
   * All configuration values are set to default values.
   *
   * @return Flink's Akka default config
   */
  def getDefaultAkkaConfig: Config = {
    getAkkaConfig(new Configuration(), Some(("", 0)))
  }

  /**
   * Gets the basic Akka config which is shared by remote and local actor systems.
   *
   * @param configuration instance which contains the user specified values for the configuration
   * @return Flink's basic Akka config
   */
  private def getBasicAkkaConfig(configuration: Configuration): Config = {
    val akkaThroughput = configuration.getInteger(ConfigConstants.AKKA_DISPATCHER_THROUGHPUT,
      ConfigConstants.DEFAULT_AKKA_DISPATCHER_THROUGHPUT)
    val lifecycleEvents = configuration.getBoolean(ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS,
      ConfigConstants.DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS)

    val jvmExitOnFatalError = if (
      configuration.getBoolean(ConfigConstants.AKKA_JVM_EXIT_ON_FATAL_ERROR, false)){
      "on"
    } else {
      "off"
    }

    val logLifecycleEvents = if (lifecycleEvents) "on" else "off"

    val logLevel = getLogLevel

    val config =
      s"""
        |akka {
        | daemonic = on
        |
        | loggers = ["akka.event.slf4j.Slf4jLogger"]
        | logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
        | log-config-on-start = off
        |
        | jvm-exit-on-fatal-error = $jvmExitOnFatalError
        |
        | serialize-messages = off
        |
        | loglevel = $logLevel
        | stdout-loglevel = OFF
        |
        | log-dead-letters = $logLifecycleEvents
        | log-dead-letters-during-shutdown = $logLifecycleEvents
        |
        | actor {
        |   guardian-supervisor-strategy = "akka.actor.StoppingSupervisorStrategy"
        |   default-dispatcher {
        |     throughput = $akkaThroughput
        |
        |     fork-join-executor {
        |       parallelism-factor = 2.0
        |     }
        |   }
        | }
        |}
      """.stripMargin

    ConfigFactory.parseString(config)
  }

  /**
   * Creates a Akka config for a remote actor system listening on port on the network interface
   * identified by hostname.
   *
   * @param configuration instance containing the user provided configuration values
   * @param hostname of the network interface to listen on
   * @param port to bind to or if 0 then Akka picks a free port automatically
   * @return Flink's Akka configuration for remote actor systems
   */
  private def getRemoteAkkaConfig(configuration: Configuration,
                                  hostname: String, port: Int): Config = {
    val akkaAskTimeout = Duration(configuration.getString(
      ConfigConstants.AKKA_ASK_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT))

    val startupTimeout = configuration.getString(
      ConfigConstants.AKKA_STARTUP_TIMEOUT,
      akkaAskTimeout.toString)

    val transportHeartbeatInterval = configuration.getString(
      ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_INTERVAL,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL)

    val transportHeartbeatPause = configuration.getString(
      ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_PAUSE,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE)

    val transportThreshold = configuration.getDouble(
      ConfigConstants.AKKA_TRANSPORT_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_THRESHOLD)

    val watchHeartbeatInterval = configuration.getString(
      ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL,
      (akkaAskTimeout/10).toString)

    val watchHeartbeatPause = configuration.getString(
      ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE,
      akkaAskTimeout.toString)

    val watchThreshold = configuration.getDouble(
      ConfigConstants.AKKA_WATCH_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_WATCH_THRESHOLD)

    val akkaTCPTimeout = configuration.getString(
      ConfigConstants.AKKA_TCP_TIMEOUT,
      akkaAskTimeout.toString)

    val akkaFramesize = configuration.getString(
      ConfigConstants.AKKA_FRAMESIZE,
      ConfigConstants.DEFAULT_AKKA_FRAMESIZE)

    val lifecycleEvents = configuration.getBoolean(
      ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS,
      ConfigConstants.DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS)

    val logLifecycleEvents = if (lifecycleEvents) "on" else "off"

    val configString =
      s"""
         |akka {
         |  actor {
         |    provider = "akka.remote.RemoteActorRefProvider"
         |  }
         |
         |  remote {
         |    startup-timeout = $startupTimeout
         |
         |    transport-failure-detector{
         |      acceptable-heartbeat-pause = $transportHeartbeatPause
         |      heartbeat-interval = $transportHeartbeatInterval
         |      threshold = $transportThreshold
         |    }
         |
         |    watch-failure-detector{
         |      heartbeat-interval = $watchHeartbeatInterval
         |      acceptable-heartbeat-pause = $watchHeartbeatPause
         |      threshold = $watchThreshold
         |    }
         |
         |    netty {
         |      tcp {
         |        transport-class = "akka.remote.transport.netty.NettyTransport"
         |        port = $port
         |        connection-timeout = $akkaTCPTimeout
         |        maximum-frame-size = $akkaFramesize
         |        tcp-nodelay = on
         |      }
         |    }
         |
         |    log-remote-lifecycle-events = $logLifecycleEvents
         |  }
         |}
       """.stripMargin

      val hostnameConfigString = if(hostname != null && hostname.nonEmpty){
        s"""
           |akka {
           |  remote {
           |    netty {
           |      tcp {
           |        hostname = $hostname
           |      }
           |    }
           |  }
           |}
         """.stripMargin
      }else{
        // if hostname is null or empty, then leave hostname unspecified. Akka will pick
        // InetAddress.getLocalHost.getHostAddress
        ""
      }

    ConfigFactory.parseString(configString + hostnameConfigString)
  }

  def getLogLevel: String = {
    if (LOG.isTraceEnabled) {
      "TRACE"
    } else {
      if (LOG.isDebugEnabled) {
        "DEBUG"
      } else {
        if (LOG.isInfoEnabled) {
          "INFO"
        } else {
          if (LOG.isWarnEnabled) {
            "WARNING"
          } else {
            if (LOG.isErrorEnabled) {
              "ERROR"
            } else {
              "OFF"
            }
          }
        }
      }
    }
  }

  def getChild(parent: ActorRef, child: String,
               system: ActorSystem,
               timeout: FiniteDuration): Future[ActorRef] = {
    system.actorSelection(parent.path / child).resolveOne()(timeout)
  }

  def getReference(path: String, system:
                   ActorSystem,
                   timeout: FiniteDuration): Future[ActorRef] = {
    system.actorSelection(path).resolveOne()(timeout)
  }


  /**
   * Utility function to construct a future which tries multiple times to execute itself if it
   * fails. If the maximum number of tries are exceeded, then the future fails.
   *
   * @param body function describing the future action
   * @param tries number of maximum tries before the future fails
   * @param executionContext which shall execute the future
   * @tparam T return type of the future
   * @return future which tries to recover by re-executing itself a given number of times
   */
  def retry[T](body: => T, tries: Int)(implicit executionContext: ExecutionContext): Future[T] = {
    Future{ body }.recoverWith{
      case t:Throwable =>
        if(tries > 0){
          retry(body, tries - 1)
        }else{
          Future.failed(t)
        }
    }
  }

  /**
   * Utility function to construct a future which tries multiple times to execute itself if it
   * fails. If the maximum number of tries are exceeded, then the future fails.
   *
   * @param callable future action
   * @param tries maximum number of tries before the future fails
   * @param executionContext which shall execute the future
   * @tparam T return type of the future
   * @return future which tries to recover by re-executing itself a given number of times
   */
  def retry[T](callable: Callable[T], tries: Int)(implicit executionContext: ExecutionContext):
  Future[T] = {
    retry(callable.call(), tries)
  }

  /**
   * Utility function to construct a future which tries multiple times to execute itself if it
   * fails. If the maximum number of tries are exceeded, then the future fails.
   *
   * @param target actor which receives the message
   * @param message to be sent to the target actor
   * @param tries maximum number of tries before the future fails
   * @param executionContext which shall execute the future
   * @param timeout of the future
   * @return future which tries to receover by re-executing itself a given number of times
   */
  def retry(target: ActorRef, message: Any, tries: Int)(implicit executionContext:
  ExecutionContext, timeout: FiniteDuration): Future[Any] = {
    (target ? message)(timeout) recoverWith{
      case t: Throwable =>
        if(tries > 0){
          retry(target, message, tries-1)
        }else{
          Future.failed(t)
        }
    }
  }

  def getTimeout(config: Configuration): FiniteDuration = {
    val duration = Duration(config.getString(ConfigConstants.AKKA_ASK_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT))

    new FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
  }

  def getDefaultTimeout: FiniteDuration = {
    val duration = Duration(ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT)

    new FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
  }

  def getLookupTimeout(config: Configuration): FiniteDuration = {
    val duration = Duration(config.getString(
      ConfigConstants.AKKA_LOOKUP_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_LOOKUP_TIMEOUT))

    new FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
  }

  def getDefaultLookupTimeout: FiniteDuration = {
    val duration = Duration(ConfigConstants.DEFAULT_AKKA_LOOKUP_TIMEOUT)
    new FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
  }
}
