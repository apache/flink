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

import akka.actor._
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID,ExecutionGraph,ExecutionVertex}
import org.apache.flink.runtime.jobgraph.{JobID,JobVertexID}
import org.apache.flink.runtime.state.OperatorState

import java.lang.Long
import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration,_}


object StreamStateMonitor {

  def props(context: ActorContext,executionGraph: ExecutionGraph,
            interval: FiniteDuration = 5 seconds): ActorRef = {

    val vertices: Iterable[ExecutionVertex] = getExecutionVertices(executionGraph)
    val monitor = context.system.actorOf(Props(new StreamStateMonitor(executionGraph,
      vertices,vertices.map(x => ((x.getJobVertex.getJobVertexId,x.getParallelSubtaskIndex),
              List.empty[Long])).toMap,Map(),interval,0L,-1L)))
    monitor ! InitBarrierScheduler
    monitor
  }

  private def getExecutionVertices(executionGraph: ExecutionGraph): Iterable[ExecutionVertex] = {
    for((_,execJobVertex) <- executionGraph.getAllVertices;
        execVertex: ExecutionVertex <- execJobVertex.getTaskVertices)
    yield execVertex
  }
}

class StreamStateMonitor(val executionGraph: ExecutionGraph,
                         val vertices: Iterable[ExecutionVertex],
                         var acks: Map[(JobVertexID,Int),List[Long]],
                         var states: Map[(JobVertexID, Integer, Long), OperatorState[_]],
                         val interval: FiniteDuration,var curId: Long,var ackId: Long)
        extends Actor with ActorLogMessages with ActorLogging {

  override def receiveWithLogMessages: Receive = {
    
    case InitBarrierScheduler =>
      context.system.scheduler.schedule(interval,interval,self,BarrierTimeout)
      context.system.scheduler.schedule(2 * interval,2 * interval,self,TriggerBarrierCompaction)
      log.debug("[FT-MONITOR] Started Stream State Monitor for job {}{}",
        executionGraph.getJobID,executionGraph.getJobName)
      
    case BarrierTimeout =>
      curId += 1
      log.debug("[FT-MONITOR] Sending Barrier to vertices of Job " + executionGraph.getJobName)
      vertices.filter(v => v.getJobVertex.getJobVertex.isInputVertex).foreach(vertex
      => vertex.getCurrentAssignedResource.getInstance.getTaskManager
                ! BarrierReq(vertex.getCurrentExecutionAttempt.getAttemptId,curId))
      
    case StateBarrierAck(jobID, jobVertexID, instanceID, checkpointID, opState) =>
      states += (jobVertexID, instanceID, checkpointID) -> opState  
      self ! BarrierAck(jobID, jobVertexID, instanceID, checkpointID)
      
    case BarrierAck(jobID, jobVertexID,instanceID,checkpointID) =>
      acks.get(jobVertexID,instanceID) match {
        case Some(acklist) =>
          acks += (jobVertexID,instanceID) -> (checkpointID :: acklist)
        case None =>
      }
      log.debug(acks.toString)
      
    case TriggerBarrierCompaction =>
      val barrierCount = acks.values.foldLeft(TreeMap[Long,Int]().withDefaultValue(0))((dict,myList)
      => myList.foldLeft(dict)((dict2,elem) => dict2.updated(elem,dict2(elem) + 1)))
      val keysToKeep = barrierCount.filter(_._2 == acks.size).keys
      ackId = if(!keysToKeep.isEmpty) keysToKeep.max else ackId
      acks.keys.foreach(x => acks = acks.updated(x,acks(x).filter(_ >= ackId)))
      states = states.filterKeys(_._3 >= ackId)
      log.debug("[FT-MONITOR] Last global barrier is " + ackId)

    case JobStateRequest =>
      sender ! JobStateResponse(executionGraph.getJobID, ackId, states)
  }
}

case class BarrierTimeout()

case class InitBarrierScheduler()

case class TriggerBarrierCompaction()

case class JobStateRequest()

case class JobStateResponse(jobID: JobID, barrierID: Long, opStates: Map[(JobVertexID, Integer, 
        Long), OperatorState[_]])

case class BarrierReq(attemptID: ExecutionAttemptID,checkpointID: Long)

case class BarrierAck(jobID: JobID,jobVertexID: JobVertexID,instanceID: Int,checkpointID: Long)

case class StateBarrierAck(jobID: JobID, jobVertexID: JobVertexID, instanceID: Integer,
                           checkpointID: Long, state: OperatorState[_])
       



