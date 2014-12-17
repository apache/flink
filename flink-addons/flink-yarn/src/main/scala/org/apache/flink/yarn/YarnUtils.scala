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

package org.apache.flink.yarn

import akka.actor.{Props, ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.taskmanager.TaskManager

object YarnUtils {
  def createActorSystem(hostname: String, port: Int, configuration: Configuration): ActorSystem = {
    val akkaConfig = ConfigFactory.parseString(AkkaUtils.getConfigString(hostname, port,
      configuration) + getConfigString)

    AkkaUtils.createActorSystem(akkaConfig)
  }

  def createActorSystem(): ActorSystem = {
    val akkaConfig = ConfigFactory.parseString(AkkaUtils.getDefaultActorSystemConfigString +
      getConfigString)

    AkkaUtils.createActorSystem(akkaConfig)
  }

  def getConfigString: String = {
    """
    |akka{
    |  loglevel = "DEBUG"
    |  stdout-loglevel = "DEBUG"
    |  log-dead-letters-during-shutdown = off
    |  log-dead-letters = off
    |
    |  actor {
    |    provider = "akka.remote.RemoteActorRefProvider"
    |  }
    |
    |  remote{
    |    log-remote-lifecycle-events = off
    |
    |    netty{
    |      tcp{
    |        transport-class = "akka.remote.transport.netty.NettyTransport"
    |        tcp-nodelay = on
    |        maximum-frame-size = 1MB
    |        execution-pool-size = 4
    |      }
    |    }
    |  }
    |}""".stripMargin
  }

  def startActorSystemAndTaskManager(args: Array[String]): (ActorSystem, ActorRef) = {
    val (hostname, port, config) = TaskManager.parseArgs(args)

    val actorSystem = createActorSystem(hostname, port, config)

    val (connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfiguration) =
      TaskManager.parseConfiguration(hostname, config, false)

    (actorSystem, TaskManager.startActor(Props(new TaskManager(connectionInfo, jobManagerURL,
      taskManagerConfig, networkConnectionConfiguration) with YarnTaskManager))(actorSystem))
  }
}
