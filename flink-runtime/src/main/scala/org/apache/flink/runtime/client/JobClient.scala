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

import akka.actor.Status.Failure
import akka.actor._
import akka.pattern.ask
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.messages.ExecutionGraphMessages.ExecutionStateChanged
import org.apache.flink.runtime.messages.JobClientMessages.{SubmitJobDetached, SubmitJobAndWait}
import org.apache.flink.runtime.messages.JobManagerMessages._

import scala.concurrent.Await
import scala.concurrent.duration.Duration


class JobClient(jobManagerURL: String) extends Actor with ActorLogMessages with ActorLogging{
  import context._

  val jobManager = AkkaUtils.getReference(jobManagerURL)

  override def receiveWithLogMessages: Receive = {
    case SubmitJobDetached(jobGraph) =>
      jobManager.tell(SubmitJob(jobGraph, registerForEvents = false, detach = true), sender())
    case cancelJob: CancelJob =>
      jobManager forward cancelJob
    case SubmitJobAndWait(jobGraph, listen) =>
      val listener = context.actorOf(Props(classOf[JobClientListener], sender()))
      jobManager.tell(SubmitJob(jobGraph, registerForEvents = listen, detach = false), listener)
    case RequestBlobManagerPort =>
      jobManager forward RequestBlobManagerPort
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

  def startActor(jobManagerURL: String)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(Props(classOf[JobClient], jobManagerURL), JOB_CLIENT_NAME)
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

        JobManager.getAkkaURL(jobManagerAddress + ":" + jobManagerRPCPort)
    }
  }

  def startActorWithConfiguration(config: Configuration)(implicit actorSystem: ActorSystem):
  ActorRef= {
    startActor(parseConfiguration(config))
  }

  @throws(classOf[JobExecutionException])
  def submitJobAndWait(jobGraph: JobGraph, listen: Boolean,
                       jobClient: ActorRef): JobExecutionResult = {
    import AkkaUtils.FUTURE_TIMEOUT
    val response = jobClient ? SubmitJobAndWait(jobGraph, listenToEvents = listen)

    Await.result(response.mapTo[JobExecutionResult],Duration.Inf)
  }


  def submitJobDetached(jobGraph: JobGraph, listen: Boolean, jobClient: ActorRef): SubmissionResponse = {
    import AkkaUtils.FUTURE_TIMEOUT
    val response = jobClient ? SubmitJobDetached(jobGraph, listenToEvents = listen)

    Await.result(response.mapTo[SubmissionResponse],AkkaUtils.FUTURE_DURATION)
  }

  @throws(classOf[IOException])
  def uploadJarFiles(jobGraph: JobGraph, hostname: String, jobClient: ActorRef): Unit = {
    val port = AkkaUtils.ask[Int](jobClient, RequestBlobManagerPort)

    val serverAddress = new InetSocketAddress(hostname, port)

    jobGraph.uploadRequiredJarFiles(serverAddress)
  }
}
