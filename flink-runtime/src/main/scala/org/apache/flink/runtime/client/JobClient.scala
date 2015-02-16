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
import java.net.{InetAddress, InetSocketAddress}

import akka.actor.Status.Failure
import akka.actor._
import akka.pattern.{Patterns, ask}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.messages.JobClientMessages.{SubmitJobDetached, SubmitJobAndWait}
import org.apache.flink.runtime.messages.JobManagerMessages._

import scala.concurrent.{TimeoutException, Await}
import scala.concurrent.duration.FiniteDuration

/**
 * Actor which constitutes the bridge between the non-actor code and the JobManager. The JobClient
 * is used to submit jobs to the JobManager and to request the port of the BlobManager.
 *
 * @param jobManager ActorRef to JobManager
 */
class JobClient(jobManager: ActorRef) extends
Actor with ActorLogMessages with ActorLogging {

  override def receiveWithLogMessages: Receive = {
    case SubmitJobDetached(jobGraph) =>
      jobManager forward SubmitJob(jobGraph, registerForEvents = false, detached = true)

    case cancelJob: CancelJob =>
      jobManager forward cancelJob

    case SubmitJobAndWait(jobGraph, listen) =>
      val listener = context.actorOf(Props(classOf[JobClientListener], sender))
      jobManager.tell(SubmitJob(jobGraph, registerForEvents = listen, detached = false), listener)

    case RequestBlobManagerPort =>
      jobManager forward RequestBlobManagerPort

    case RequestJobManagerStatus =>
      jobManager forward RequestJobManagerStatus
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }
}

/**
 * Helper actor which listens to status messages from the JobManager and prints them on the
 * standard output. Such an actor is started for each job, which is configured to listen to these
 * status messages.
 *
 * @param jobSubmitter Akka URL of the sender of the job
 */
class JobClientListener(jobSubmitter: ActorRef) extends Actor with ActorLogMessages with
ActorLogging {
  override def receiveWithLogMessages: Receive = {
    case SubmissionFailure(jobID, t) =>
      System.out.println(s"Submission of job with ID $jobID was unsuccessful, " +
        s"because ${t.getMessage}.")

    case SubmissionSuccess(_) =>

    case JobResultSuccess(_, duration, accumulatorResults) =>
      jobSubmitter ! new JobExecutionResult(duration, accumulatorResults)
      self ! PoisonPill

    case JobResultCanceled(_, t) =>
      jobSubmitter ! Failure(new JobCancellationException("The job has been cancelled.", t))
      self ! PoisonPill

    case JobResultFailed(_, t) =>
      jobSubmitter ! Failure(new JobExecutionException("The job execution failed.", t))
      self ! PoisonPill

    case msg =>
      // we have to use System.out.println here to avoid erroneous behavior for output redirection
      System.out.println(msg.toString)
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }
}

/**
 * JobClient's companion object containing convenience functions to start a JobClient actor, parse
 * the configuration to extract the JobClient's settings and convenience functions to submit jobs.
 */
object JobClient {

  val JOB_CLIENT_NAME = "jobclient"

  @throws(classOf[IOException])
  def startActorSystemAndActor(config: Configuration,
                               localActorSystem: Boolean): (ActorSystem, ActorRef) = {

    // start a remote actor system to listen on an arbitrary port
    val actorSystem = AkkaUtils.createActorSystem(configuration = config,
                                                  listeningAddress = Some(("", 0)))
    try {
      val jobClientActor = createJobClientFromConfig(config, localActorSystem, actorSystem)
      (actorSystem, jobClientActor)
    }
    catch {
      case t: Throwable => {
        actorSystem.shutdown()
        throw t
      }
    }
  }

  @throws(classOf[IOException])
  def createJobClientFromConfig(config: Configuration,
                                localActorSystem: Boolean,
                                actorSystem: ActorSystem): ActorRef = {

    val jobManagerAddress = getJobManagerUrlFromConfig(config, localActorSystem)
    createJobClient(jobManagerAddress, actorSystem, config)
  }

  @throws(classOf[IOException])
  def createJobClient(jobManagerURL: String,
                      actorSystem: ActorSystem,
                      config: Configuration): ActorRef = {

    val timeout = AkkaUtils.getLookupTimeout(config)
    val jobManager = JobManager.getJobManagerRemoteReference(jobManagerURL, actorSystem, timeout)

    actorSystem.actorOf(Props(classOf[JobClient], jobManager), JOB_CLIENT_NAME)
  }


  /**
   * Extracts the JobManager's Akka URL from the configuration. If localActorSystem is true, then
   * the JobClient is executed in the same actor system as the JobManager. Thus, they can
   * communicate locally.
   *
   * @param configuration Configuration object containing all user provided configuration values
   * @param localActorSystem  true if the JobClient runs in the same actor system as the JobManager,
   *                          otherwise false
   * @return Akka URL of the JobManager
   */
  def getJobManagerUrlFromConfig(configuration: Configuration,
                                 localActorSystem: Boolean): String = {
    if (localActorSystem) {
      // JobManager and JobClient run in the same ActorSystem
      JobManager.getLocalJobManagerAkkaURL
    } else {
      val jobManagerAddress = configuration.getString(
        ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)

      val jobManagerRPCPort = configuration.getInteger(
        ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
        ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

      if (jobManagerAddress == null) {
        throw new RuntimeException(
          "JobManager address has not been specified in the configuration.")
      }

      val hostPort = new InetSocketAddress(InetAddress.getByName(jobManagerAddress),
                                           jobManagerRPCPort)
      JobManager.getRemoteJobManagerAkkaURL(hostPort)
    }
  }

  /**
   * Sends a [[JobGraph]] to the JobClient actor specified by jobClient which submits it then to
   * the JobManager. The method blocks until the job has finished or the JobManager is no longer
   * alive. In the former case, the [[JobExecutionResult]] is returned and in the latter case a
   * [[JobExecutionException]] is thrown.
   *
   * @param jobGraph JobGraph describing the Flink job
   * @param listenToStatusEvents true if the JobClient shall print status events of the
   *                             corresponding job, otherwise false
   * @param jobClient ActorRef to the JobClient
   * @param timeout Timeout for futures
   * @throws org.apache.flink.runtime.client.JobExecutionException Thrown if the job
   *                                                               execution fails.
   * @return The job execution result
   */
  @throws(classOf[Exception])
  def submitJobAndWait(jobGraph: JobGraph, listenToStatusEvents: Boolean, jobClient: ActorRef)
                      (implicit timeout: FiniteDuration): JobExecutionResult = {

    var waitForAnswer = true
    var answer: JobExecutionResult = null

    val result = (jobClient ? SubmitJobAndWait(jobGraph, listenToEvents = listenToStatusEvents))(
      AkkaUtils.INF_TIMEOUT).mapTo[JobExecutionResult]

    while (waitForAnswer) {
      try {
        answer = Await.result(result, timeout)
        waitForAnswer = false
      } catch {
        case x: TimeoutException =>
          val jmStatus = (jobClient ? RequestJobManagerStatus)(timeout).mapTo[JobManagerStatus]

          try {
            Await.result(jmStatus, timeout)
          } catch {
            case t: Throwable =>
              throw new JobTimeoutException("Lost connection to job manager.", t)
          }
      }
    }

    answer
  }

  /**
   * Submits a job in detached mode. The method sends the corresponding [[JobGraph]] to the
   * JobClient specified by jobClient. The JobClient does not start a [[JobClientListener]] and
   * simply returns the [[SubmissionResponse]] of the [[JobManager]]. The SubmissionResponse is
   * then returned by this method.
   *
   * @param jobGraph Flink job
   * @param jobClient ActorRef to the JobClient
   * @param timeout Tiemout for futures
   * @return The submission response
   */
  @throws(classOf[Exception])
  def submitJobDetached(jobGraph: JobGraph, jobClient: ActorRef)(implicit timeout: FiniteDuration):
  SubmissionResponse = {
    val response = (jobClient ? SubmitJobDetached(jobGraph))(timeout)

    try {
      Await.result(response.mapTo[SubmissionResponse], timeout)
    } catch {
      case timeout: TimeoutException =>
        throw new JobTimeoutException("Timeout while waiting for the submission result.", timeout);
      case t: Throwable =>
        throw new JobExecutionException("Exception while waiting for the submission result.", t)
    }
  }

  /**
   * Uploads the specified jar files of the [[JobGraph]] jobGraph to the BlobServer of the
   * JobManager. The respective port is retrieved from the JobManager. This function issues a
   * blocking call.
   *
   * @param jobGraph Flink job containing the information about the required jars
   * @param hostname Hostname of the instance on which the BlobServer and also the JobManager run
   * @param jobClient ActorRef to the JobClient
   * @param timeout Timeout for futures
   * @throws IOException Thrown, if the file upload to the JobManager failed.
   */
  @throws(classOf[IOException])
  def uploadJarFiles(jobGraph: JobGraph, hostname: String, jobClient: ActorRef)(
    implicit timeout: FiniteDuration): Unit = {

    if (jobGraph.hasUsercodeJarFiles()) {
      val futureBlobPort = Patterns.ask(jobClient, RequestBlobManagerPort, timeout).mapTo[Int]

      val port = try {
        Await.result(futureBlobPort, timeout)
      } catch {
        case e: Exception => throw new IOException("Could not retrieve the server's blob port.", e)
      }

      val serverAddress = new InetSocketAddress(hostname, port)

      jobGraph.uploadRequiredJarFiles(serverAddress)
    }
  }
}
