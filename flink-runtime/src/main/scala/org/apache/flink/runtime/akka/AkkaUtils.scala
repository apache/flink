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

import java.io.IOException
import java.util.concurrent.Callable

import akka.actor.{ActorSelection, ActorRef, ActorSystem}
import akka.pattern.{Patterns, ask => akkaAsk}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._

object AkkaUtils {
  val DEFAULT_TIMEOUT: FiniteDuration = 1 minute

  val INF_TIMEOUT = 21474835 seconds

  var globalExecutionContext: ExecutionContext = ExecutionContext.global

  def createActorSystem(host: String, port: Int, configuration: Configuration): ActorSystem = {
    val akkaConfig = ConfigFactory.parseString(AkkaUtils.getConfigString(host, port, configuration))
    createActorSystem(akkaConfig)
  }

  def createActorSystem(): ActorSystem = {
    createActorSystem(getDefaultActorSystemConfig)
  }

  def createLocalActorSystem(): ActorSystem = {
    createActorSystem(getDefaultLocalActorSystemConfig)
  }

  def createActorSystem(akkaConfig: Config): ActorSystem = {
    ActorSystem.create("flink", akkaConfig)
  }

  def getConfigString(host: String, port: Int, configuration: Configuration): String = {
    val startupTimeout = configuration.getString(ConfigConstants.AKKA_STARTUP_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_STARTUP_TIMEOUT)
    val transportHeartbeatInterval = configuration.getString(ConfigConstants.
      AKKA_TRANSPORT_HEARTBEAT_INTERVAL,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL)
    val transportHeartbeatPause = configuration.getString(ConfigConstants.
      AKKA_TRANSPORT_HEARTBEAT_PAUSE,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE)
    val transportThreshold = configuration.getDouble(ConfigConstants.AKKA_TRANSPORT_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_THRESHOLD)
    val watchHeartbeatInterval = configuration.getString(ConfigConstants
      .AKKA_WATCH_HEARTBEAT_INTERVAL, ConfigConstants.DEFAULT_AKKA_WATCH_HEARTBEAT_INTERVAL)
    val watchHeartbeatPause = configuration.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE,
      ConfigConstants.DEFAULT_AKKA_WATCH_HEARTBEAT_PAUSE)
    val watchThreshold = configuration.getDouble(ConfigConstants.AKKA_WATCH_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_WATCH_THRESHOLD)
    val akkaTCPTimeout = configuration.getString(ConfigConstants.AKKA_TCP_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_TCP_TIMEOUT)
    val akkaFramesize = configuration.getString(ConfigConstants.AKKA_FRAMESIZE,
      ConfigConstants.DEFAULT_AKKA_FRAMESIZE)
    val akkaThroughput = configuration.getInteger(ConfigConstants.AKKA_DISPATCHER_THROUGHPUT,
      ConfigConstants.DEFAULT_AKKA_DISPATCHER_THROUGHPUT)
    val lifecycleEvents = configuration.getBoolean(ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS,
      ConfigConstants.DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS)

    val logLifecycleEvents = if (lifecycleEvents) "on" else "off"

    val logLevel = configuration.getString(ConfigConstants.AKKA_LOG_LEVEL,
      ConfigConstants.DEFAULT_AKKA_LOG_LEVEL)

    val configString =
      s"""
         |akka {
         |  loglevel = $logLevel
         |  stdout-loglevel = $logLevel
         |
         |  log-dead-letters = $logLifecycleEvents
         |  log-dead-letters-during-shutdown = $logLifecycleEvents
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
         |    netty{
         |      tcp{
         |        hostname = $host
         |        port = $port
         |        connection-timeout = $akkaTCPTimeout
         |        maximum-frame-size = ${akkaFramesize}
         |      }
         |    }
         |
         |    log-remote-lifecycle-events = $logLifecycleEvents
         |  }
         |
         |  actor{
         |    default-dispatcher{
         |
         |      throughput = ${akkaThroughput}
         |
         |      fork-join-executor {
         |        parallelism-factor = 2.0
         |      }
         |    }
         |  }
         |
         |}
       """.stripMargin

    getDefaultActorSystemConfigString + configString
  }

  def getLocalConfigString(configuration: Configuration): String = {
    val akkaThroughput = configuration.getInteger(ConfigConstants.AKKA_DISPATCHER_THROUGHPUT,
      ConfigConstants.DEFAULT_AKKA_DISPATCHER_THROUGHPUT)
    val lifecycleEvents = configuration.getBoolean(ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS,
      ConfigConstants.DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS)

    val logLifecycleEvents = if (lifecycleEvents) "on" else "off"

    val logLevel = configuration.getString(ConfigConstants.AKKA_LOG_LEVEL,
      ConfigConstants.DEFAULT_AKKA_LOG_LEVEL)

    val configString =
      s"""
         |akka {
         |  loglevel = $logLevel
         |  stdout-loglevel = $logLevel
         |
         |  log-dead-letters = $logLifecycleEvents
         |  log-dead-letters-during-shutdown = $logLifecycleEvents
         |
         |  actor{
         |    default-dispatcher{
         |
         |      throughput = ${akkaThroughput}
         |
         |      fork-join-executor {
         |        parallelism-factor = 2.0
         |      }
         |    }
         |  }
         |
         |}
       """.stripMargin

    getDefaultLocalActorSystemConfigString + configString
  }

  def getDefaultActorSystemConfigString: String = {
    val config = """
       |akka {
       |  actor {
       |    provider = "akka.remote.RemoteActorRefProvider"
       |  }
       |
       |  remote{
       |    netty{
       |      tcp{
       |        port = 0
       |        transport-class = "akka.remote.transport.netty.NettyTransport"
       |        tcp-nodelay = on
       |        maximum-frame-size = 1MB
       |        execution-pool-size = 4
       |      }
       |    }
       |  }
       |}
     """.stripMargin

    getDefaultLocalActorSystemConfigString + config
  }

  def getDefaultLocalActorSystemConfigString: String = {
    """
      |akka {
      |  daemonic = on
      |
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  logger-startup-timeout = 30s
      |  loglevel = "WARNING"
      |  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
      |  stdout-loglevel = "WARNING"
      |  jvm-exit-on-fatal-error = off
      |  log-config-on-start = off
      |  serialize-messages = on
      |}
    """.stripMargin
  }

  def getDefaultActorSystemConfig = {
    ConfigFactory.parseString(getDefaultActorSystemConfigString)
  }

  def getDefaultLocalActorSystemConfig = {
    ConfigFactory.parseString(getDefaultLocalActorSystemConfigString)
  }

  def getChild(parent: ActorRef, child: String)(implicit system: ActorSystem, timeout:
  FiniteDuration): ActorRef = {
    Await.result(system.actorSelection(parent.path / child).resolveOne()(timeout), timeout)
  }

  def getReference(path: String)(implicit system: ActorSystem, timeout: FiniteDuration): ActorRef
  = {
    Await.result(system.actorSelection(path).resolveOne()(timeout), timeout)
  }

  @throws(classOf[IOException])
  def ask[T](actorSelection: ActorSelection, msg: Any)(implicit timeout: FiniteDuration): T
    = {
    val future = Patterns.ask(actorSelection, msg, timeout)
    Await.result(future, timeout).asInstanceOf[T]
  }

  @throws(classOf[IOException])
  def ask[T](actor: ActorRef, msg: Any)(implicit timeout: FiniteDuration): T = {
    val future = Patterns.ask(actor, msg, timeout)
    Await.result(future, timeout).asInstanceOf[T]
  }

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

  def retry[T](callable: Callable[T], tries: Int)(implicit executionContext: ExecutionContext):
  Future[T] = {
    retry(callable.call(), tries)
  }

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
}
