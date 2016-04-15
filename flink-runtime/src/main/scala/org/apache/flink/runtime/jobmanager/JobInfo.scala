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
  val client: ActorRef,
  val listeningBehaviour: ListeningBehaviour,
  val start: Long,
  val sessionTimeout: Long) extends Serializable {

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

  override def toString = s"JobInfo(client: $client ($listeningBehaviour), start: $start)"

  def setLastActive() =
    lastActive = System.currentTimeMillis()
}

object JobInfo{
  def apply(
    client: ActorRef,
    listeningBehaviour: ListeningBehaviour,
    start: Long,
    sessionTimeout: Long) = new JobInfo(client, listeningBehaviour, start, sessionTimeout)
}
