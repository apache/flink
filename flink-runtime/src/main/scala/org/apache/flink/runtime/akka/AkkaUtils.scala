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
import akka.pattern.Patterns
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.core.io.IOReadableWritable
import org.apache.flink.runtime.akka.serialization.{WritableSerializer,
IOReadableWritableSerializer}
import org.apache.hadoop.io.Writable
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._

object AkkaUtils {
  implicit val FUTURE_TIMEOUT: Timeout = 1 minute
  implicit val AWAIT_DURATION: FiniteDuration = 1 minute
  implicit val FUTURE_DURATION: FiniteDuration = 1 minute

  var globalExecutionContext: ExecutionContext = ExecutionContext.global

  def createActorSystem(host: String, port: Int, configuration: Configuration): ActorSystem = {
    val akkaConfig = ConfigFactory.parseString(AkkaUtils.getConfigString(host, port, configuration))
    createActorSystem(akkaConfig)
  }

  def createActorSystem(): ActorSystem = {
    createActorSystem(getDefaultActorSystemConfig)
  }

  def createActorSystem(akkaConfig: Config): ActorSystem = {
    ActorSystem.create("flink", akkaConfig)
  }

  def getConfigString(host: String, port: Int, configuration: Configuration): String = {
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

    val configString = s"""akka.remote.transport-failure-detector.heartbeat-interval =
                       $transportHeartbeatInterval
       |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $transportHeartbeatPause
       |akka.remote.transport-failure-detector.threshold = $transportThreshold
       |akka.remote.watch-failure-detector.heartbeat-interval = $watchHeartbeatInterval
       |akka.remote.watch-failure-detector.acceptable-heartbeat-pause = $watchHeartbeatPause
       |akka.remote.wathc-failure-detector.threshold = $watchThreshold
       |akka.remote.netty.tcp.hostname = $host
       |akka.remote.netty.tcp.port = $port
       |akka.remote.netty.tcp.connection-timeout = $akkaTCPTimeout
       |akka.remote.netty.tcp.maximum-frame-size = $akkaFramesize
       |akka.actor.default-dispatcher.throughput = $akkaThroughput
       |akka.remote.log-remote-lifecycle-events = $logLifecycleEvents
       |akka.log-dead-letters = $logLifecycleEvents
       |akka.log-dead-letters-during-shutdown = $logLifecycleEvents
       |akka.loglevel = "$logLevel"
       |akka.stdout-loglevel = "$logLevel"
     """.stripMargin

    getDefaultActorSystemConfigString + configString
  }

  def getDefaultActorSystemConfigString: String = {
    s"""akka.daemonic = on
      |akka.loggers = ["akka.event.slf4j.Slf4jLogger"]
      |akka.loglevel = "WARNING"
      |akka.logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
      |akka.stdout-loglevel = "WARNING"
      |akka.jvm-exit-on-fata-error = off
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.log-config-on-start = off
      |akka.remote.netty.tcp.port = 0
      |akka.remote.netty.tcp.maximum-frame-size = 1MB
    """.stripMargin
  }

  def getDefaultActorSystemConfig = {
    ConfigFactory.parseString(getDefaultActorSystemConfigString)
  }

  def getChild(parent: ActorRef, child: String)(implicit system: ActorSystem): ActorRef = {
    Await.result(system.actorSelection(parent.path / child).resolveOne(), AWAIT_DURATION)
  }

  def getReference(path: String)(implicit system: ActorSystem): ActorRef = {
    Await.result(system.actorSelection(path).resolveOne(), AWAIT_DURATION)
  }

  @throws(classOf[IOException])
  def ask[T](actorSelection: ActorSelection, msg: Any): T = {
    ask(actorSelection, msg, FUTURE_TIMEOUT, FUTURE_DURATION)
  }

  @throws(classOf[IOException])
  def ask[T](actor: ActorRef, msg: Any): T = {
    ask(actor, msg, FUTURE_TIMEOUT, FUTURE_DURATION)
  }

  @throws(classOf[IOException])
  def ask[T](actorSelection: ActorSelection, msg: Any, timeout: Timeout, duration: Duration): T = {
    val future = Patterns.ask(actorSelection, msg, timeout)
    Await.result(future, duration).asInstanceOf[T]
  }

  @throws(classOf[IOException])
  def ask[T](actor: ActorRef, msg: Any, timeout: Timeout, duration: Duration): T = {
    val future = Patterns.ask(actor, msg, timeout)
    Await.result(future, duration).asInstanceOf[T]
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
}
