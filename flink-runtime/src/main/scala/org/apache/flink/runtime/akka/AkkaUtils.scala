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

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.core.io.IOReadableWritable
import org.apache.flink.runtime.akka.serialization.IOReadableWritableSerializer
import scala.concurrent.Await
import scala.concurrent.duration._

object AkkaUtils {
  implicit val FUTURE_TIMEOUT: Timeout = 1 minute
  implicit val AWAIT_DURATION: Duration = 1 minute

  def createActorSystem(host: String, port: Int, configuration: Configuration): ActorSystem = {
    val akkaConfig = ConfigFactory.parseString(AkkaUtils.getConfigString(host, port, configuration))
    val actorSystem = ActorSystem.create("flink", akkaConfig)

    actorSystem
  }

  def getChild(parent: ActorRef, child: String)(implicit system: ActorSystem): ActorRef = {
    Await.result(system.actorSelection(parent.path / child).resolveOne(), AWAIT_DURATION)
  }

  def getConfigString(host: String, port: Int, configuration: Configuration): String = {
    val transportHeartbeatInterval = configuration.getString(ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_INTERVAL,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL)
    val transportHeartbeatPause = configuration.getString(ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_PAUSE,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE)
    val transportThreshold = configuration.getDouble(ConfigConstants.AKKA_TRANSPORT_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_THRESHOLD)
    val watchHeartbeatInterval = configuration.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL,
      ConfigConstants.DEFAULT_AKKA_WATCH_HEARTBEAT_INTERVAL)
    val watchHeartbeatPause = configuration.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE,
      ConfigConstants.DEFAULT_AKKA_WATCH_HEARTBEAT_PAUSE)
    val watchThreshold = configuration.getDouble(ConfigConstants.AKKA_WATCH_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_WATCH_THRESHOLD)
    val akkaTCPTimeout = configuration.getString(ConfigConstants.AKKA_TCP_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_TCP_TIMEOUT)
    val akkaFramesize = configuration.getString(ConfigConstants.AKKA_FRAMESIZE, ConfigConstants.DEFAULT_AKKA_FRAMESIZE)
    val akkaThroughput = configuration.getInteger(ConfigConstants.AKKA_DISPATCHER_THROUGHPUT,
      ConfigConstants.DEFAULT_AKKA_DISPATCHER_THROUGHPUT)
    val lifecycleEvents = configuration.getBoolean(ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS,
      ConfigConstants.DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS)

    val logLifecycleEvents = if(lifecycleEvents) "on" else "off"

    val ioRWSerializerClass = classOf[IOReadableWritableSerializer].getCanonicalName
    val ioRWClass = classOf[IOReadableWritable].getCanonicalName

    s"""akka.daemonic = on
       |akka.loggers = ["akka.event.slf4j.Slf4jLogger"]
       |akka.loglevel = "DEBUG"
       |akka.logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
       |akka.stdout-logleve = "DEBUG"
       |akka.jvm-exit-on-fatal-error = off
       |akka.remote.transport-failure-detector.heartbeat-interval = $transportHeartbeatInterval
       |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $transportHeartbeatPause
       |akka.remote.transport-failure-detector.threshold = $transportThreshold
       |akka.remote.watch-failure-detector.heartbeat-interval = $watchHeartbeatInterval
       |akka.remote.watch-failure-detector.acceptable-heartbeat-pause = $watchHeartbeatPause
       |akka.remote.wathc-failure-detector.threshold = $watchThreshold
       |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
       |akka.remote.netty.tcp.hostname = $host
       |akka.remote.netty.tcp.port = $port
       |akka.remote.netty.tcp.tcp-nodelay = on
       |akka.remote.netty.tcp.connection-timeout = $akkaTCPTimeout
       |akka.remote.netty.tcp.maximum-frame-size = $akkaFramesize
       |akka.actor.default-dispatcher.throughput = $akkaThroughput
       |akka.log-config-on-start = on
       |akka.remote.log-remote-lifecycle-events = $logLifecycleEvents
       |akka.log-dead-letters = $logLifecycleEvents
       |akka.log-dead-letters-during-shutdown = $logLifecycleEvents
       |akka.actor.serializers {
       |  IOReadableWritable = "$ioRWSerializerClass"
       |}
       |akka.actor.serialization-bindings {
       |  "$ioRWClass" = IOReadableWritable
       |}
     """.stripMargin
  }
}