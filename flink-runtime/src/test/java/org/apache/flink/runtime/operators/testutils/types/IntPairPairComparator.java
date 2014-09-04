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
import org.apache.flink.runtime.messages.RegistrationMessages.{AcknowledgeRegistration, RegisteredTaskManager}

import org.apache.flink.api.common.typeutils.TypePairComparator;

  override def receive: Receive = {
    case AcknowledgeRegistration =>
      println("Got registered at " + sender().toString())
      Thread.sleep(1000)
      self ! PoisonPill
  }
}

public class IntPairPairComparator extends TypePairComparator<IntPair, IntPair> {
	
	private int key;
	

  def startActorSystemAndActor(systemName: String, hostname: String, port: Int, actorName: String,
                               configuration: Configuration) = {
    val actorSystem = AkkaUtils.createActorSystem(systemName, hostname, port, configuration)
    startActor(actorSystem, actorName)
    actorSystem
  }

  def startActor(actorSystem: ActorSystem, actorName: String): ActorRef = {
    actorSystem.actorOf(Props(classOf[TaskManager]), actorName);
  }
}
