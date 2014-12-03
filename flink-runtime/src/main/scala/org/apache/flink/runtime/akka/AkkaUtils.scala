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

import java.io.IOException
import java.util.concurrent.Callable

import akka.actor.{ActorSelection, ActorRef, ActorSystem}
import akka.pattern.{Patterns, ask => akkaAsk}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._

object AkkaUtils {
  val DEFAULT_TIMEOUT: FiniteDuration = 1 minute

  val INF_TIMEOUT = 21474835 seconds

  var globalExecutionContext: ExecutionContext = ExecutionContext.global

  def createActorSystem(host: String, port: Int, configuration: Configuration): ActorSystem = {
    val akkaConfig = ConfigFactory.parseString(AkkaUtils.getConfigString(host, port, configuration))
    createActorSystem(akkaConfig)
  }

  def createActorSystem(): ActorSystem = {
    createActorSystem(getDefaultActorSystemConfig)
  }

  def createActorSystem(akkaConfig: Config): ActorSystem = {
    ActorSystem.create("flink", akkaConfig)
  }

  def getConfigString(host: String, port: Int, configuration: Configuration): String = {
    val transportHeartbeatInterval = configuration.getString(ConfigConstants.
      AKKA_TRANSPORT_HEARTBEAT_INTERVAL,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL)
    val transportHeartbeatPause = configuration.getString(ConfigConstants.
      AKKA_TRANSPORT_HEARTBEAT_PAUSE,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE)
    val transportThreshold = configuration.getDouble(ConfigConstants.AKKA_TRANSPORT_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_TRANSPORT_THRESHOLD)
    val watchHeartbeatInterval = configuration.getString(ConfigConstants
      .AKKA_WATCH_HEARTBEAT_INTERVAL, ConfigConstants.DEFAULT_AKKA_WATCH_HEARTBEAT_INTERVAL)
    val watchHeartbeatPause = configuration.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE,
      ConfigConstants.DEFAULT_AKKA_WATCH_HEARTBEAT_PAUSE)
    val watchThreshold = configuration.getDouble(ConfigConstants.AKKA_WATCH_THRESHOLD,
      ConfigConstants.DEFAULT_AKKA_WATCH_THRESHOLD)
    val akkaTCPTimeout = configuration.getString(ConfigConstants.AKKA_TCP_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_TCP_TIMEOUT)
    val akkaFramesize = configuration.getString(ConfigConstants.AKKA_FRAMESIZE,
      ConfigConstants.DEFAULT_AKKA_FRAMESIZE)
    val akkaThroughput = configuration.getInteger(ConfigConstants.AKKA_DISPATCHER_THROUGHPUT,
      ConfigConstants.DEFAULT_AKKA_DISPATCHER_THROUGHPUT)
    val lifecycleEvents = configuration.getBoolean(ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS,
      ConfigConstants.DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS)

    val logLifecycleEvents = if (lifecycleEvents) "on" else "off"

    val logLevel = configuration.getString(ConfigConstants.AKKA_LOG_LEVEL,
      ConfigConstants.DEFAULT_AKKA_LOG_LEVEL)

    val configString =
      s"""
         |akka {
         |  loglevel = $logLevel
         |  stdout-loglevel = $logLevel
         |
         |  log-dead-letters = $logLifecycleEvents
         |  log-dead-letters-during-shutdown = $logLifecycleEvents
         |
         |  remote {
         |    transport-failure-detector{
         |      acceptable-heartbeat-pause = $transportHeartbeatPause
         |      heartbeat-interval = $transportHeartbeatInterval
         |      threshold = $transportThreshold
         |    }
         |
         |    watch-failure-detector{
         |      heartbeat-interval = $watchHeartbeatInterval
         |      acceptable-heartbeat-pause = $watchHeartbeatPause
         |      threshold = $watchThreshold
         |    }
         |
         |    netty{
         |      tcp{
         |        hostname = $host
         |        port = $port
         |        connection-timeout = $akkaTCPTimeout
         |        maximum-frame-size = ${akkaFramesize}
         |      }
         |    }
         |
         |    log-remote-lifecycle-events = $logLifecycleEvents
         |  }
         |
         |  actor{
         |    default-dispatcher{
         |      throughput = ${akkaThroughput}
         |    }
         |  }
         |
         |}
       """.stripMargin

    getDefaultActorSystemConfigString + configString
  }

  def getDefaultActorSystemConfigString: String = {
    """
       |akka {
       |  daemonic = on
       |
       |  loggers = ["akka.event.slf4j.Slf4jLogger"]
       |  loglevel = "WARNING"
       |  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
       |  stdout-loglevel = "WARNING"
       |  jvm-exit-on-fatal-error = off
       |  log-config-on-start = on
       |  serialize-messages = on
       |
       |  actor {
       |    provider = "akka.remote.RemoteActorRefProvider"
       |  }
       |
       |  remote{
       |    netty{
       |      tcp{
       |        port = 0
       |        transport-class = "akka.remote.transport.netty.NettyTransport"
       |        tcp-nodelay = on
       |        maximum-frame-size = 1MB
       |        execution-pool-size = 4
       |      }
       |    }
       |  }
       |}
     """.stripMargin
    }

  // scalastyle:off line.size.limit

  def getKryoSerializerString: String = {
    """
      |akka {
      |
      |  extensions = ["com.romix.akka.serialization.kryo.KryoSerializationExtension$"]
      |
      |  actor{
      |    kryo{
      |      type = "graph"
      |      idstrategy = "incremental"
      |      serializer-pool-size = 16
      |      buffer-size = 4096
      |      max-buffer-size = -1
      |      use-manifests = false
      |      compression = off
      |      implicit-registration-logging = true
      |      kryo-trace = false
      |      kryo-custom-serializer-init = "org.apache.flink.runtime.akka.KryoInitializer"
      |    }
      |
      |
      |    serializers{
      |      kryo = "com.romix.akka.serialization.kryo.KryoSerializer"
      |      java = "akka.serialization.JavaSerializer"
      |    }
      |
      |    serialization-bindings {
      |      "java.io.Serializable" = java
      |
      |      "java.lang.Throwable" = java
      |      "akka.event.Logging$Error" = java
      |      "java.lang.Integer" = kryo
      |      "java.lang.Long" = kryo
      |      "java.lang.Float" = kryo
      |      "java.lang.Double" = kryo
      |      "java.lang.Boolean" = kryo
      |      "java.lang.Short" = kryo
      |
      |      "scala.Tuple2" = kryo
      |      "scala.Tuple3" = kryo
      |      "scala.Tuple4" = kryo
      |      "scala.Tuple5" = kryo
      |      "scala.Tuple6" = kryo
      |      "scala.Tuple7" = kryo
      |      "scala.Tuple8" = kryo
      |      "scala.Tuple9" = kryo
      |      "scala.Tuple10" = kryo
      |      "scala.Tuple11" = kryo
      |      "scala.Tuple12" = kryo
      |      "scala.collection.BitSet" = kryo
      |      "scala.collection.SortedSet" = kryo
      |      "scala.util.Left" = kryo
      |      "scala.util.Right" = kryo
      |      "scala.collection.SortedMap" = kryo
      |      "scala.Int" = kryo
      |      "scala.Long" = kryo
      |      "scala.Float" = kryo
      |      "scala.Double" = kryo
      |      "scala.Boolean" = kryo
      |      "scala.Short" = kryo
      |      "java.lang.String" = kryo
      |      "scala.Option" = kryo
      |      "scala.collection.immutable.Map" = kryo
      |      "scala.collection.Traversable" = kryo
      |      "scala.runtime.BoxedUnit" = kryo
      |
      |      "akka.actor.SystemGuardian$RegisterTerminationHook$" = kryo
      |      "akka.actor.Address" = kryo
      |      "akka.actor.Terminated" = kryo
      |      "akka.actor.LocalActorRef" = kryo
      |      "akka.actor.RepointableActorRef" = kryo
      |      "akka.actor.Identify" = kryo
      |      "akka.actor.ActorIdentity" = kryo
      |      "akka.actor.PoisonPill$" = kryo
      |      "akka.actor.SystemGuardian$TerminationHook$" = kryo
      |      "akka.actor.SystemGuardian$TerminationHookDone$" = kryo
      |      "akka.actor.AddressTerminated" = kryo
      |      "akka.actor.Status$Failure" = kryo
      |      "akka.remote.RemoteWatcher$ReapUnreachableTick$" = kryo
      |      "akka.remote.RemoteWatcher$HeartbeatTick$" = kryo
      |      "akka.remote.ReliableDeliverySupervisor$GotUid" = kryo
      |      "akka.remote.EndpointWriter$AckIdleCheckTimer$" = kryo
      |      "akka.remote.EndpointWriter$StoppedReading" = kryo
      |      "akka.remote.ReliableDeliverySupervisor$Ungate$" = kryo
      |      "akka.remote.EndpointWriter$StopReading" = kryo
      |      "akka.remote.EndpointWriter$OutboundAck" = kryo
      |      "akka.remote.Ack" = kryo
      |      "akka.remote.SeqNo" = kryo
      |      "akka.remote.EndpointWriter$FlushAndStop$" = kryo
      |      "akka.remote.ReliableDeliverySupervisor$AttemptSysMsgRedelivery$" = kryo
      |      "akka.remote.RemoteWatcher$WatchRemote" = kryo
      |      "akka.remote.RemoteWatcher$UnwatchRemote" = kryo
      |      "akka.remote.RemoteWatcher$RewatchRemote" = kryo
      |      "akka.remote.RemoteWatcher$Rewatch" = kryo
      |      "akka.remote.RemoteWatcher$Heartbeat$" = kryo
      |      "akka.remote.RemoteWatcher$HeartbeatRsp" = kryo
      |      "akka.remote.EndpointWriter$FlushAndStopTimeout$" = kryo
      |      "akka.remote.RemoteWatcher$ExpectedFirstHeartbeat" = kryo
      |      "akka.remote.transport.Transport$InvalidAssociationException" = kryo
      |      "akka.dispatch.sysmsg.Terminate" = kryo
      |      "akka.dispatch.sysmsg.Unwatch" = kryo
      |      "akka.dispatch.sysmsg.Watch" = kryo
      |      "akka.dispatch.sysmsg.DeathWatchNotification" = kryo
      |
      |      "org.apache.flink.runtime.messages.ArchiveMessages$ArchiveExecutionGraph" = kryo
      |      "org.apache.flink.runtime.messages.ArchiveMessages$ArchivedJobs" = kryo
      |
      |      "org.apache.flink.runtime.messages.ExecutionGraphMessages$ExecutionStateChanged" = kryo
      |      "org.apache.flink.runtime.messages.ExecutionGraphMessages$JobStatusChanged" = kryo
      |
      |      "org.apache.flink.runtime.messages.JobClientMessages$SubmitJobAndWait" = kryo
      |      "org.apache.flink.runtime.messages.JobClientMessages$SubmitJobDetached" = kryo
      |
      |      "org.apache.flink.runtime.messages.JobManagerMessages$SubmitJob" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$SubmissionSuccess" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$SubmissionFailure" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$CancellationSuccess" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$CancellationFailure" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$CancelJob" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$UpdateTaskExecutionState" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestNextInputSplit" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$LookupConnectionInformation" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$ConnectionInformation" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$ReportAccumulatorResult" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestAccumulatorResults" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$AccumulatorResultsFound" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$AccumulatorResultsNotFound" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestJobStatus" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$CurrentJobStatus" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestNumberRegisteredTaskManager$" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestTotalNumberOfSlots$" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestBlobManagerPort$" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestFinalJobStatus" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$JobResultSuccess" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$JobResultCanceled" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$JobResultFailed" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestRunningJobs$" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RunningJobs" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestJob" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$JobFound" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$JobNotFound" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RequestRegisteredTaskManagers$" = kryo
      |      "org.apache.flink.runtime.messages.JobManagerMessages$RegisteredTaskManagers" = kryo
      |
      |      "org.apache.flink.runtime.messages.JobManagerProfilerMessages$ReportProfilingData" = kryo
      |
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$NotifyWhenRegisteredAtJobManager$" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$RegisterAtJobManager$" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$CancelTask" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$SubmitTask" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$NextInputSplit" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$UnregisterTask" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$TaskOperationResult" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$Heartbeat" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$RegisteredAtJobManager$" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$RegisterAtJobManager$" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$SendHeartbeat$" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerMessages$LogMemoryUsage$" = kryo
      |
      |      "org.apache.flink.runtime.messages.TaskManagerProfilerMessages$MonitorTask" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerProfilerMessages$UnmonitorTask" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerProfilerMessages$RegisterProfilingListener$" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerProfilerMessages$UnregisterProfilingListener$" = kryo
      |      "org.apache.flink.runtime.messages.TaskManagerProfilerMessages$ProfileTasks$" = kryo
      |
      |      "org.apache.flink.runtime.messages.RegistrationMessages$RegisterTaskManager" = kryo
      |      "org.apache.flink.runtime.messages.RegistrationMessages$AcknowledgeRegistration" = kryo
      |    }
      |  }
      |}
    """.stripMargin
  }

  // scalastyle:on line.size.limit

  def getDefaultActorSystemConfig = {
    ConfigFactory.parseString(getDefaultActorSystemConfigString)
  }

  def getChild(parent: ActorRef, child: String)(implicit system: ActorSystem, timeout:
  FiniteDuration): ActorRef = {
    Await.result(system.actorSelection(parent.path / child).resolveOne()(timeout), timeout)
  }

  def getReference(path: String)(implicit system: ActorSystem, timeout: FiniteDuration): ActorRef
  = {
    Await.result(system.actorSelection(path).resolveOne()(timeout), timeout)
  }

  @throws(classOf[IOException])
  def ask[T](actorSelection: ActorSelection, msg: Any)(implicit timeout: FiniteDuration): T
    = {
    val future = Patterns.ask(actorSelection, msg, timeout)
    Await.result(future, timeout).asInstanceOf[T]
  }

  @throws(classOf[IOException])
  def ask[T](actor: ActorRef, msg: Any)(implicit timeout: FiniteDuration): T = {
    val future = Patterns.ask(actor, msg, timeout)
    Await.result(future, timeout).asInstanceOf[T]
  }

  def askInf[T](actor: ActorRef, msg: Any): T = {
    val future = Patterns.ask(actor, msg, INF_TIMEOUT)
    Await.result(future, INF_TIMEOUT).asInstanceOf[T]
  }

  def retry[T](body: => T, tries: Int)(implicit executionContext: ExecutionContext): Future[T] = {
    Future{ body }.recoverWith{
      case t:Throwable =>
        if(tries > 0){
          retry(body, tries - 1)
        }else{
          Future.failed(t)
        }
    }
  }

  def retry[T](callable: Callable[T], tries: Int)(implicit executionContext: ExecutionContext):
  Future[T] = {
    retry(callable.call(), tries)
  }

  def retry(target: ActorRef, message: Any, tries: Int)(implicit executionContext:
  ExecutionContext, timeout: FiniteDuration): Future[Any] = {
    (target ? message)(timeout) recoverWith{
      case t: Throwable =>
        if(tries > 0){
          retry(target, message, tries-1)
        }else{
          Future.failed(t)
        }
    }
  }
}
