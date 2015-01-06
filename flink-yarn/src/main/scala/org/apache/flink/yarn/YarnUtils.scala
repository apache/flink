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
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.taskmanager.TaskManager
import org.slf4j.LoggerFactory

object YarnUtils {

  val LOG = LoggerFactory.getLogger(this.getClass)

  def startActorSystemAndTaskManager(args: Array[String]): (ActorSystem, ActorRef) = {
    val (hostname, port, config) = TaskManager.parseArgs(args)

    val actorSystem = AkkaUtils.createActorSystem(config, Some((hostname, port)))

    val (connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfiguration) =
      TaskManager.parseConfiguration(hostname, config, localAkkaCommunication = false,
        localTaskManagerCommunication = false)

    (actorSystem, TaskManager.startActor(Props(new TaskManager(connectionInfo, jobManagerURL,
      taskManagerConfig, networkConnectionConfiguration) with YarnTaskManager))(actorSystem))
  }
}
