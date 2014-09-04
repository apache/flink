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


import akka.actor._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.messages.JobmanagerMessages.RequestNumberRegisteredTaskManager
import org.apache.flink.runtime.messages.RegistrationMessages._

import scala.collection.mutable

class JobManager extends Actor with ActorLogging {

  val taskManagers = new mutable.HashSet[ActorRef]()

  override def receive: Receive = {
    case RegisterTaskManager(hardwareInformation) =>
      val taskManager = sender()
      taskManagers += taskManager
      context.watch(taskManager);
      taskManager ! AcknowledgeRegistration

    case RequestNumberRegisteredTaskManager =>
      sender() ! taskManagers.size
  }
}

object JobManager{
  def startActorSystemAndActor(systemName: String, hostname: String,  port: Int, actorName: String,
                               configuration: Configuration): ActorSystem = {
    val actorSystem = AkkaUtils.createActorSystem(systemName, hostname,  port, configuration)
    startActor(actorSystem, actorName)
    actorSystem
  }

  def startActor(actorSystem: ActorSystem, actorName: String): ActorRef = {
    actorSystem.actorOf(Props(classOf[JobManager]), actorName)
  }
}
