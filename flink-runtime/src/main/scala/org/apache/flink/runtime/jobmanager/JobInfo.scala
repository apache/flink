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

package org.apache.flink.runtime.jobmanager

import akka.actor.ActorRef
import org.apache.flink.runtime.akka.ListeningBehaviour


/**
 * Utility class to store job information on the [[JobManager]]. The JobInfo stores which actor
 * submitted the job, when the start time and, if already terminated, the end time was.
 * Additionally, it stores whether the job was started in the detached mode. Detached means that
 * the submitting actor does not wait for the job result once the job has terminated.
 *
 * Important: This class is serializable, but needs to be deserialized in the context of an actor
 * system in order to resolve the client [[ActorRef]]. It is possible to serialize the Akka URL
 * manually, but it is cumbersome and complicates testing in certain scenarios, where you need to
 * make sure to resolve the correct [[ActorRef]]s when submitting jobs (RepointableActorRef vs.
 * RemoteActorRef).
 *
 * @param client Actor which submitted the job
 * @param start Starting time
 */
class JobInfo(
  client: ActorRef,
  listeningBehaviour: ListeningBehaviour,
  val start: Long,
  val sessionTimeout: Long) extends Serializable {

  val clients = scala.collection.mutable.HashSet[(ActorRef, ListeningBehaviour)]()
  clients += ((client, listeningBehaviour))

  var sessionAlive = sessionTimeout > 0

  var lastActive = 0L

  setLastActive()

  var end: Long = -1

  def duration: Long = {
    if(end != -1){
      end - start
    }else{
      -1
    }
  }


  /**
    * Notifies all clients by sending a message
    * @param message the message to send
    */
  def notifyClients(message: Any) = {
    clients foreach {
      case (clientActor, _) =>
        clientActor ! message
    }
  }

  /**
    * Notifies all clients which are not of type detached
    * @param message the message to sent to non-detached clients
    */
  def notifyNonDetachedClients(message: Any) = {
    clients foreach {
      case (clientActor, ListeningBehaviour.DETACHED) =>
        // do nothing
      case (clientActor, _) =>
        clientActor ! message
    }
  }

  /**
    * Sends a message to job clients that match the listening behavior
    * @param message the message to send to all clients
    * @param listeningBehaviour the desired listening behaviour
    */
  def notifyClients(message: Any, listeningBehaviour: ListeningBehaviour) = {
    clients foreach {
      case (clientActor, `listeningBehaviour`) =>
        clientActor ! message
      case _ =>
    }
  }

  def setLastActive() =
    lastActive = System.currentTimeMillis()


  override def toString = s"JobInfo(clients: ${clients.toString()}, start: $start)"

  override def equals(other: Any): Boolean = other match {
    case that: JobInfo =>
      clients == that.clients &&
        start == that.start &&
        sessionTimeout == that.sessionTimeout
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(clients, start, sessionTimeout)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object JobInfo{
  def apply(
    client: ActorRef,
    listeningBehaviour: ListeningBehaviour,
    start: Long,
    sessionTimeout: Long) = new JobInfo(client, listeningBehaviour, start, sessionTimeout)
}
