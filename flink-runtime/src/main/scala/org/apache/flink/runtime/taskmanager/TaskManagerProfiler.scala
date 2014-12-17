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

package org.apache.flink.runtime.taskmanager

import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit

import akka.actor.{Cancellable, ActorRef, Actor, ActorLogging}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.execution.{RuntimeEnvironment, ExecutionState}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.messages.ExecutionGraphMessages.ExecutionStateChanged
import org.apache.flink.runtime.messages.JobManagerProfilerMessages.ReportProfilingData
import org.apache.flink.runtime.messages.TaskManagerProfilerMessages._
import org.apache.flink.runtime.profiling.ProfilingException
import org.apache.flink.runtime.profiling.impl.types.ProfilingDataContainer
import org.apache.flink.runtime.profiling.impl.{EnvironmentThreadSet, InstanceProfiler}

import scala.concurrent.duration.FiniteDuration

class TaskManagerProfiler(val instancePath: String, val reportInterval: Int) extends Actor with
ActorLogMessages with ActorLogging {

  import context.dispatcher

  val tmx = ManagementFactory.getThreadMXBean
  val instanceProfiler = new InstanceProfiler(instancePath)
  val listeners = scala.collection.mutable.Set[ActorRef]()
  val environments = scala.collection.mutable.HashMap[ExecutionAttemptID, RuntimeEnvironment]()
  val monitoredThreads = scala.collection.mutable.HashMap[RuntimeEnvironment,
    EnvironmentThreadSet]()

  var monitoringScheduler: Option[Cancellable] = None

  if (tmx.isThreadContentionMonitoringSupported) {
    tmx.setThreadContentionMonitoringEnabled(true)
  } else {
    throw new ProfilingException("The thread contention monitoring is not supported.")
  }


  override def receiveWithLogMessages: Receive = {
    case MonitorTask(task) => {
      task.registerExecutionListener(self)
      environments += task.getExecutionId -> task.getEnvironment
    }

    case UnmonitorTask(executionAttemptID) => {
      environments.remove(executionAttemptID)
    }

    case RegisterProfilingListener => {
      listeners += sender
      if (monitoringScheduler.isEmpty) {
        startMonitoring
      }
    }

    case UnregisterProfilingListener => {
      listeners -= sender
      if (listeners.isEmpty) {
        stopMonitoring
      }
    }

    case ProfileTasks => {
      val timestamp = System.currentTimeMillis()

      val profilingDataContainer = new ProfilingDataContainer()

      for ((env, set) <- monitoredThreads) {
        val threadProfilingData = set.captureCPUUtilization(env.getJobID, tmx, timestamp)

        if (threadProfilingData != null) {
          profilingDataContainer.addProfilingData(threadProfilingData)
        }

        if (monitoredThreads.nonEmpty) {
          val instanceProfilingData = try {
            Some(instanceProfiler.generateProfilingData(timestamp))
          } catch {
            case e: ProfilingException => {
              log.error(e, "Error while retrieving instance profiling data.")
              None
            }
          }

          instanceProfilingData foreach {
            profilingDataContainer.addProfilingData(_)
          }

          if (!profilingDataContainer.isEmpty) {
            for (listener <- listeners) {
              listener ! ReportProfilingData(profilingDataContainer)
            }
          }

          profilingDataContainer.clear()
        }
      }
    }

    case ExecutionStateChanged(_, vertexID, _, _, subtaskIndex, executionID, newExecutionState,
    _, _) => {
      import ExecutionState._

      environments.get(executionID) match {
        case Some(environment) =>
          newExecutionState match {
            case RUNNING => registerMainThreadForCPUProfiling(environment, vertexID,
              subtaskIndex, executionID)
            case FINISHED | CANCELING | CANCELED | FAILED =>
              unregisterMainThreadFromCPUProfiling(environment)
            case _ =>
          }
        case None =>
          log.warning(s"Could not find environment for execution id ${executionID}.")
      }
    }
  }

  def startMonitoring(): Unit = {
    val interval = new FiniteDuration(reportInterval, TimeUnit.MILLISECONDS)
    val delay = new FiniteDuration((reportInterval * Math.random()).toLong, TimeUnit.MILLISECONDS)
    monitoringScheduler = Some(context.system.scheduler.schedule(delay, interval, self,
      ProfileTasks))
  }

  def stopMonitoring(): Unit = {
    monitoringScheduler.foreach {
      _.cancel()
    }
    monitoringScheduler = None
  }

  def registerMainThreadForCPUProfiling(environment: RuntimeEnvironment, vertexID: JobVertexID,
                                        subtask: Int,
                                        executionID: ExecutionAttemptID): Unit = {
    monitoredThreads += environment -> new EnvironmentThreadSet(tmx,
      environment.getExecutingThread, vertexID,
      subtask, executionID)
  }

  def unregisterMainThreadFromCPUProfiling(environment: RuntimeEnvironment): Unit = {
    monitoredThreads.remove(environment) match {
      case Some(set) =>
        if (set.getMainThread != environment.getExecutingThread) {
          log.error(s"The thread ${environment.getExecutingThread.getName} is not the main thread" +
            s" of this environment.")
        }
      case None =>
    }
  }
}
