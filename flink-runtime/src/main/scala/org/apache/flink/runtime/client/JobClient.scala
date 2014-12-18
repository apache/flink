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

package org.apache.flink.runtime.client

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.Status.Failure
import akka.actor._
import akka.pattern.ask
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.messages.JobClientMessages.{SubmitJobDetached, SubmitJobAndWait}
import org.apache.flink.runtime.messages.JobManagerMessages._

import scala.concurrent.{TimeoutException, Await}
import scala.concurrent.duration.{FiniteDuration}


class JobClient(jobManagerURL: String, timeout: FiniteDuration) extends Actor with ActorLogMessages
with  ActorLogging{
  import context._

  val jobManager = AkkaUtils.getReference(jobManagerURL)(system, timeout)

  override def receiveWithLogMessages: Receive = {
    case SubmitJobDetached(jobGraph) =>
      jobManager.tell(SubmitJob(jobGraph, registerForEvents = false, detach = true), sender)
    case cancelJob: CancelJob =>
      jobManager forward cancelJob
    case SubmitJobAndWait(jobGraph, listen) =>
      val listener = context.actorOf(Props(classOf[JobClientListener], sender))
      jobManager.tell(SubmitJob(jobGraph, registerForEvents = listen, detach = false), listener)
    case RequestBlobManagerPort =>
      jobManager forward RequestBlobManagerPort
    case RequestJobManagerStatus => {
      jobManager forward RequestJobManagerStatus
    }
  }
}

class JobClientListener(client: ActorRef) extends Actor with ActorLogMessages with ActorLogging {
  override def receiveWithLogMessages: Receive = {
    case SubmissionFailure(_, t) =>
      client ! Failure(t)
      self ! PoisonPill
    case SubmissionSuccess(_) =>
    case JobResultSuccess(_, duration, accumulatorResults) =>
      client ! new JobExecutionResult(duration, accumulatorResults)
      self ! PoisonPill
    case JobResultCanceled(_, msg) =>
      client ! Failure(new JobExecutionException(msg, true))
      self ! PoisonPill
    case JobResultFailed(_, msg) =>
      client ! Failure(new JobExecutionException(msg, false))
      self ! PoisonPill
    case msg =>
      println(msg.toString)
  }
}

object JobClient{
  val JOB_CLIENT_NAME = "jobclient"

  def startActorSystemAndActor(config: Configuration): (ActorSystem, ActorRef) = {
    implicit val actorSystem = AkkaUtils.createActorSystem(host = "localhost",
      port =0, configuration = config)

    (actorSystem, startActorWithConfiguration(config))
  }

  def startActor(jobManagerURL: String)(implicit actorSystem: ActorSystem, timeout: FiniteDuration):
  ActorRef = {
    actorSystem.actorOf(Props(classOf[JobClient], jobManagerURL, timeout), JOB_CLIENT_NAME)
  }

  def parseConfiguration(configuration: Configuration): String = {
    configuration.getString(ConfigConstants.JOB_MANAGER_AKKA_URL, null) match {
      case url: String => url
      case _ =>
        val jobManagerAddress = configuration.getString(ConfigConstants
          .JOB_MANAGER_IPC_ADDRESS_KEY, null);
        val jobManagerRPCPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

        if (jobManagerAddress == null) {
          throw new RuntimeException("JobManager address has not been specified in the " +
            "configuration.")
        }

        JobManager.getRemoteAkkaURL(jobManagerAddress + ":" + jobManagerRPCPort)
    }
  }

  def startActorWithConfiguration(config: Configuration)(implicit actorSystem: ActorSystem):
  ActorRef = {
    implicit val timeout = FiniteDuration(config.getInteger(ConfigConstants.AKKA_ASK_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT), TimeUnit.SECONDS)

    startActor(parseConfiguration(config))
  }

  @throws(classOf[JobExecutionException])
  def submitJobAndWait(jobGraph: JobGraph, listen: Boolean, jobClient: ActorRef)
                      (implicit timeout: FiniteDuration): JobExecutionResult = {
    var waitForAnswer = true
    var answer: JobExecutionResult = null

    val result =
      (jobClient ? SubmitJobAndWait(jobGraph, listenToEvents = listen))(AkkaUtils.INF_TIMEOUT).
        mapTo[JobExecutionResult]

    while(waitForAnswer) {
      try {
        answer = Await.result(result, timeout)
        waitForAnswer = false
      } catch {
        case x: TimeoutException => {
          val jmStatus = (jobClient ? RequestJobManagerStatus)(timeout).mapTo[JobManagerStatus]

          try {
            Await.result(jmStatus, timeout)
          } catch {
            case t: Throwable => {
              throw new JobExecutionException("JobManager not reachable anymore. Terminate " +
                "waiting for job answer.", false)
            }
          }
        }
      }
    }

    answer
  }


  def submitJobDetached(jobGraph: JobGraph, jobClient: ActorRef)(implicit timeout: FiniteDuration):
  SubmissionResponse = {
    val response = (jobClient ? SubmitJobDetached(jobGraph))(timeout)

    Await.result(response.mapTo[SubmissionResponse],timeout)
  }

  @throws(classOf[IOException])
  def uploadJarFiles(jobGraph: JobGraph, hostname: String, jobClient: ActorRef)(implicit timeout:
   FiniteDuration): Unit = {
    val port = AkkaUtils.ask[Int](jobClient, RequestBlobManagerPort)

    val serverAddress = new InetSocketAddress(hostname, port)

    jobGraph.uploadRequiredJarFiles(serverAddress)
  }
}
