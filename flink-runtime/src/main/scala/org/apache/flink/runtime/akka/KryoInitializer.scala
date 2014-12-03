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

package org.apache.flink.runtime.akka

import java.net.Inet4Address

import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.esotericsoftware.kryo.{Serializer, Kryo}
import org.apache.flink.api.common.accumulators.Accumulator
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.core.io.{LocatableInputSplit, GenericInputSplit}
import org.apache.flink.runtime.accumulators.AccumulatorEvent
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, ExecutionGraph}
import org.apache.flink.runtime.instance.{Instance, InstanceID, HardwareDescription,
InstanceConnectionInfo}
import org.apache.flink.runtime.io.network.{RemoteReceiver, ConnectionInfoLookupResponse}
import org.apache.flink.runtime.io.network.channels.ChannelID
import org.apache.flink.runtime.jobgraph.{JobVertexID, JobStatus, JobID, JobGraph}
import org.apache.flink.runtime.messages.ArchiveMessages.{ArchivedJobs, RequestArchivedJobs,
ArchiveExecutionGraph}
import org.apache.flink.runtime.messages.ExecutionGraphMessages.{ExecutionStateChanged,
JobStatusChanged}
import org.apache.flink.runtime.messages.JobClientMessages.{SubmitJobDetached, SubmitJobAndWait}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.JobManagerProfilerMessages.ReportProfilingData
import org.apache.flink.runtime.messages.RegistrationMessages.{AcknowledgeRegistration,
RegisterTaskManager}
import org.apache.flink.runtime.messages.TaskManagerMessages._
import org.apache.flink.runtime.messages.TaskManagerProfilerMessages._
import org.apache.flink.runtime.profiling.impl.types.ProfilingDataContainer
import org.apache.flink.runtime.taskmanager.{Task, TaskExecutionState}

class KryoInitializer {
  def customize(kryo: Kryo): Unit = {

    register(kryo)
  }

  def register(kryo: Kryo): Unit = {
    def register(className: String): Unit = {
      kryo.register(Class.forName(className))
    }

    def registerClass(clazz: Class[_], serializer: Serializer[_] = null): Unit = {
      if(serializer != null){
        kryo.register(clazz, serializer)
      }else {
        kryo.register(clazz)
      }
    }

    register("scala.Some")
    register("scala.None$")
    register("scala.collection.immutable.Set$EmptySet$")
    register("scala.runtime.BoxedUnit")

    register("akka.actor.SystemGuardian$RegisterTerminationHook$")
    register("akka.actor.Address")
    register("akka.actor.Terminated")
    register("akka.actor.LocalActorRef")
    register("akka.actor.RepointableActorRef")
    register("akka.actor.Identify")
    register("akka.actor.ActorIdentity")
    register("akka.actor.PoisonPill$")
    register("akka.actor.AddressTerminated")
    register("akka.actor.Status$Failure")
    register("akka.remote.RemoteWatcher$ReapUnreachableTick$")
    register("akka.remote.RemoteWatcher$HeartbeatTick$")
    register("akka.remote.ReliableDeliverySupervisor$GotUid")
    register("akka.remote.EndpointWriter$AckIdleCheckTimer$")
    register("akka.remote.EndpointWriter$StoppedReading")
    register("akka.remote.ReliableDeliverySupervisor$Ungate$")
    register("akka.remote.EndpointWriter$StopReading")
    register("akka.remote.EndpointWriter$OutboundAck")
    register("akka.remote.Ack")
    register("akka.remote.SeqNo")
    register("akka.remote.RemoteWatcher$HeartbeatRsp")
    register("akka.actor.SystemGuardian$TerminationHook$")
    register("akka.actor.SystemGuardian$TerminationHookDone$")
    register("akka.remote.EndpointWriter$FlushAndStop$")
    register("akka.remote.RemoteWatcher$WatchRemote")
    register("akka.remote.RemoteWatcher$UnwatchRemote")
    register("akka.remote.RemoteWatcher$Rewatch")
    register("akka.remote.RemoteWatcher$RewatchRemote")
    register("akka.remote.ReliableDeliverySupervisor$AttemptSysMsgRedelivery$")
    register("akka.remote.RemoteActorRef")
    register("akka.remote.RemoteWatcher$Heartbeat$")
    register("akka.remote.EndpointWriter$FlushAndStopTimeout$")
    register("akka.remote.RemoteWatcher$ExpectedFirstHeartbeat")
    register("akka.remote.transport.Transport$InvalidAssociationException")
    register("akka.remote.transport.AkkaProtocolException")
    register("akka.dispatch.sysmsg.Terminate")
    register("akka.dispatch.sysmsg.Unwatch")
    register("akka.dispatch.sysmsg.Watch")
    register("akka.dispatch.sysmsg.DeathWatchNotification")

//    register("java.util.Collections$UnmodifiableRandomAccessList")


    //Register Flink messages

    kryo.setDefaultSerializer(classOf[JavaSerializer])

    //misc types
    registerClass(classOf[JobID])
    registerClass(classOf[JobVertexID])
    registerClass(classOf[ExecutionAttemptID])
    registerClass(classOf[InstanceID])
    registerClass(classOf[ExecutionState])
    registerClass(classOf[JobStatus])
    registerClass(classOf[TaskExecutionState])
    registerClass(classOf[InstanceConnectionInfo])
    registerClass(classOf[HardwareDescription])
    registerClass(classOf[Inet4Address])
    registerClass(classOf[ChannelID])
    registerClass(classOf[ConnectionInfoLookupResponse])
    registerClass(classOf[RemoteReceiver])
    registerClass(classOf[AccumulatorEvent], new JavaSerializer)
    registerClass(classOf[Instance], new JavaSerializer())
    registerClass(classOf[JobGraph], new JavaSerializer())
    registerClass(classOf[TaskDeploymentDescriptor], new JavaSerializer())
    registerClass(classOf[ExecutionGraph], new JavaSerializer())
    registerClass(classOf[ProfilingDataContainer], new JavaSerializer)
    registerClass(classOf[Task], new JavaSerializer)
    registerClass(classOf[GenericInputSplit], new JavaSerializer)
    registerClass(classOf[LocatableInputSplit], new JavaSerializer)
    registerClass(classOf[FileInputSplit], new JavaSerializer)
    registerClass(classOf[StackTraceElement])
    registerClass(classOf[Array[StackTraceElement]])

    //Archive messages
    registerClass(classOf[ArchiveExecutionGraph])
    registerClass(RequestArchivedJobs.getClass)
    registerClass(classOf[ArchivedJobs])

    //ExecutionGraph messages
    registerClass(classOf[ExecutionStateChanged])
    registerClass(classOf[JobStatusChanged])

    //JobClient messages
    registerClass(classOf[SubmitJobAndWait])
    registerClass(classOf[SubmitJobDetached])

    // JobManager messages
    registerClass(classOf[SubmitJob])
    registerClass(classOf[SubmissionSuccess])
    registerClass(classOf[SubmissionFailure])
    registerClass(classOf[CancelJob])
    registerClass(classOf[UpdateTaskExecutionState])
    registerClass(classOf[RequestNextInputSplit])
    registerClass(classOf[LookupConnectionInformation])
    registerClass(classOf[ConnectionInformation])
    registerClass(classOf[ReportAccumulatorResult])
    registerClass(classOf[RequestAccumulatorResults])
    registerClass(classOf[AccumulatorResultsFound])
    registerClass(classOf[AccumulatorResultsNotFound])
    registerClass(classOf[RequestJobStatus])
    registerClass(classOf[CurrentJobStatus])
    registerClass(RequestNumberRegisteredTaskManager.getClass)
    registerClass(RequestTotalNumberOfSlots.getClass)
    registerClass(RequestBlobManagerPort.getClass)
    registerClass(classOf[RequestFinalJobStatus])
    registerClass(classOf[JobResultSuccess])
    registerClass(classOf[JobResultCanceled])
    registerClass(classOf[JobResultFailed])
    registerClass(classOf[CancellationSuccess])
    registerClass(classOf[CancellationFailure])
    registerClass(RequestRunningJobs.getClass)
    registerClass(classOf[RunningJobs])
    registerClass(classOf[RequestJob])
    registerClass(classOf[JobFound])
    registerClass(classOf[JobNotFound])
    registerClass(RequestRegisteredTaskManagers.getClass)
    registerClass(classOf[RegisteredTaskManagers])

    //JobManagerProfiler messages
    registerClass(classOf[ReportProfilingData])

    //Registration messages
    registerClass(classOf[RegisterTaskManager])
    registerClass(classOf[AcknowledgeRegistration])

    //TaskManager messages
    registerClass(classOf[CancelTask])
    registerClass(classOf[SubmitTask])
    registerClass(classOf[NextInputSplit])
    registerClass(classOf[UnregisterTask])
    registerClass(classOf[TaskOperationResult])
    registerClass(NotifyWhenRegisteredAtJobManager.getClass)
    registerClass(RegisterAtJobManager.getClass)
    registerClass(RegisteredAtJobManager.getClass)
    registerClass(SendHeartbeat.getClass)
    registerClass(classOf[Heartbeat])
    registerClass(LogMemoryUsage.getClass)

    //TaskManagerProfiler messages
    registerClass(classOf[MonitorTask])
    registerClass(classOf[UnmonitorTask])
    registerClass(RegisterProfilingListener.getClass)
    registerClass(UnregisterProfilingListener.getClass)
    registerClass(ProfileTasks.getClass)
  }
}
