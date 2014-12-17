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

package org.apache.flink.runtime.testingUtils

import akka.actor.{ActorRef, Props}
import akka.pattern.{ask, pipe}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.jobmanager.{JobManager, MemoryArchivist}
import org.apache.flink.runtime.messages.ExecutionGraphMessages.{JobStatusChanged,
ExecutionStateChanged}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.NotifyWhenTaskRemoved

import scala.collection.convert.WrapAsScala
import scala.concurrent.{Await, Future}


trait TestingJobManager extends ActorLogMessages with WrapAsScala {
  that: JobManager =>

  val waitForAllVerticesToBeRunning = scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()

  override def archiveProps = Props(new MemoryArchivist(archiveCount) with TestingMemoryArchivist)

  abstract override def receiveWithLogMessages: Receive = {
    receiveTestingMessages orElse super.receiveWithLogMessages
  }

  def receiveTestingMessages: Receive = {
    case RequestExecutionGraph(jobID) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) => sender ! ExecutionGraphFound(jobID,
          executionGraph)
        case None => archive.tell(RequestExecutionGraph(jobID), sender)
      }
    case WaitForAllVerticesToBeRunning(jobID) =>
      if(checkIfAllVerticesRunning(jobID)){
        sender ! AllVerticesRunning(jobID)
      }else{
        currentJobs.get(jobID) match {
          case Some((eg, _)) => eg.registerExecutionListener(self)
          case None =>
        }
        val waiting = waitForAllVerticesToBeRunning.getOrElse(jobID, Set[ActorRef]())
        waitForAllVerticesToBeRunning += jobID -> (waiting + sender)
      }
    case ExecutionStateChanged(jobID, _, _, _, _, _, _, _, _) =>
      val cleanup = waitForAllVerticesToBeRunning.get(jobID) match {
        case Some(listeners) if checkIfAllVerticesRunning(jobID) =>
          for(listener <- listeners){
            listener ! AllVerticesRunning(jobID)
          }
          true
        case _ => false
      }

      if(cleanup){
        waitForAllVerticesToBeRunning.remove(jobID)
      }
    case NotifyWhenJobRemoved(jobID) => {
      val tms = instanceManager.getAllRegisteredInstances.map(_.getTaskManager)

      val responses = tms.map{
        tm =>
          (tm ? NotifyWhenJobRemoved(jobID))(timeout).mapTo[Boolean]
      }

      import context.dispatcher

      Future.fold(responses)(true)(_ & _) pipeTo sender
    }

  }

  def checkIfAllVerticesRunning(jobID: JobID): Boolean = {
    currentJobs.get(jobID) match {
      case Some((eg, _)) =>
        eg.getAllExecutionVertices.forall( _.getExecutionState == ExecutionState.RUNNING)
      case None => false
    }
  }
}
