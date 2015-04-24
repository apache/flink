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

import java.lang.{Long => JLong}

import akka.actor._
import org.apache.flink.runtime.{ActorSynchronousLogging, ActorLogMessages}
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.executiongraph.{ExecutionGraph, ExecutionVertex}
import org.apache.flink.runtime.jobgraph.JobStatus._
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.messages.CheckpointingMessages._
import org.apache.flink.runtime.state.StateHandle

import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap
import scala.concurrent.duration.{FiniteDuration, _}

/**
 * The StreamCheckpointCoordinator is responsible for operator state management and checkpoint
 * coordination in streaming jobs. It periodically sends checkpoint barriers to the sources of a
 * running job and constantly collects acknowledgements from operators while the barriers are being 
 * disseminated throughout the execution graph. Upon time intervals it finds the last globally
 * acknowledged checkpoint barrier to be used for a consistent recovery and loads all associated 
 * state handles to the respected execution vertices.
 * 
 * The following messages describe this actor's expected behavior: 
 *
 *  - [[InitBarrierScheduler]] initiates the actor and schedules the periodic [[BarrierTimeout]] 
 *  and [[CompactAndUpdate]] messages that are used for maintaining the state checkpointing logic. 
 *
 *  - [[BarrierTimeout]] is periodically triggered upon initiation in order to start a new 
 *  checkpoint barrier. That is when the barriers are being disseminated to the source vertices.
 *
 *  - [[BarrierAck]] is being sent by each operator upon the completion of a state checkpoint. All
 *  such acknowledgements are being collected and inspected upon [[CompactAndUpdate]] handling in
 *  order to find out the last consistent checkpoint.
 *  
 *  - [[StateBarrierAck]] describes an acknowledgement such as the case of a [[BarrierAck]] that 
 *  additionally carries operatorState with it.
 *
 * - [[CompactAndUpdate]] marks the last globally consistent checkpoint barrier to be used for 
 * recovery purposes and removes all older states and acknowledgements up to that barrier.
 * Furthermore, it updates the current ExecutionGraph with the current operator state handles 
 * 
 */

class StreamCheckpointCoordinator(val executionGraph: ExecutionGraph,
                                  val vertices: Iterable[ExecutionVertex],
                                  var acks: Map[(JobVertexID,Int),List[JLong]],
                                  var states: Map[(JobVertexID, Integer, JLong), StateHandle],
                                  val interval: FiniteDuration,
                                  var curId: JLong,
                                  var ackId: JLong)
extends Actor with ActorLogMessages with ActorSynchronousLogging {

  implicit private val executor = context.dispatcher

  override def receiveWithLogMessages: Receive = {

    case InitBarrierScheduler =>
      context.system.scheduler.schedule(interval,interval,self,BarrierTimeout)
      context.system.scheduler.schedule(2 * interval,2 * interval,self,CompactAndUpdate)
      log.info("Started Stream State Monitor for job " +
        s"${executionGraph.getJobID}${executionGraph.getJobName}")
      
    case BarrierTimeout =>
      executionGraph.getState match {
        case FAILED | CANCELED | FINISHED =>
          log.info(s"Stopping monitor for terminated job ${executionGraph.getJobID}.")
          self ! PoisonPill
        case RUNNING =>
          curId += 1
          log.debug("Sending Barrier to vertices of Job " + executionGraph.getJobName)
          vertices.filter(v => v.getJobVertex.getJobVertex.isInputVertex &&
                  v.getExecutionState == ExecutionState.RUNNING).foreach(vertex
          => vertex.getCurrentAssignedResource.getInstance.getTaskManager
                    ! BarrierReq(vertex.getCurrentExecutionAttempt.getAttemptId,curId))
        case _ =>
          log.debug("Omitting sending barrier since graph is in " +
            s"${executionGraph.getState} state for job ${executionGraph.getJobID}.")
      }
      
    case StateBarrierAck(jobID, jobVertexID, instanceID, checkpointID, opState) =>
      states += (jobVertexID, instanceID, checkpointID) -> opState
      self ! BarrierAck(jobID, jobVertexID, instanceID, checkpointID)
      
    case BarrierAck(jobID, jobVertexID,instanceID,checkpointID) =>
          acks.get(jobVertexID,instanceID) match {
            case Some(acklist) =>
              acks += (jobVertexID,instanceID) -> (checkpointID :: acklist)
            case None =>
          }
          log.debug(acks.toString())
      
    case CompactAndUpdate =>
      val barrierCount =
        acks.values.foldLeft(TreeMap[JLong,Int]().withDefaultValue(0))((dict,myList)
      => myList.foldLeft(dict)((dict2,elem) => dict2.updated(elem,dict2(elem) + 1)))
      val keysToKeep = barrierCount.filter(_._2 == acks.size).keys
      ackId = if(keysToKeep.nonEmpty) keysToKeep.max else ackId
      acks.keys.foreach(x => acks = acks.updated(x,acks(x).filter(_ >= ackId)))
      states = states.filterKeys(_._3 >= ackId)
      log.debug("[FT-MONITOR] Last global barrier is " + ackId)
      executionGraph.loadOperatorStates(states)
      
  }
}

object StreamCheckpointCoordinator {

  def spawn(context: ActorContext,executionGraph: ExecutionGraph,
            interval: FiniteDuration = 5 seconds): ActorRef = {

    val vertices: Iterable[ExecutionVertex] = getExecutionVertices(executionGraph)
    val monitor = context.system.actorOf(Props(new StreamCheckpointCoordinator(executionGraph,
      vertices,vertices.map(x => ((x.getJobVertex.getJobVertexId,x.getParallelSubtaskIndex),
              List.empty[JLong])).toMap, Map() ,interval,0L,-1L)))
    monitor ! InitBarrierScheduler
    monitor
  }

  private def getExecutionVertices(executionGraph: ExecutionGraph): Iterable[ExecutionVertex] = {
    for((_,execJobVertex) <- executionGraph.getAllVertices;
        execVertex: ExecutionVertex <- execJobVertex.getTaskVertices)
    yield execVertex
  }
}

case class BarrierTimeout()

case class InitBarrierScheduler()

case class CompactAndUpdate()
