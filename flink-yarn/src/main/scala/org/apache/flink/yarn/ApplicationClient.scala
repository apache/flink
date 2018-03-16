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

package org.apache.flink.yarn

import java.util.UUID

import akka.actor._
import grizzled.slf4j.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.clusterframework.messages._
import org.apache.flink.runtime.leaderretrieval.{LeaderRetrievalListener, LeaderRetrievalService}
import org.apache.flink.runtime.{FlinkActor, LeaderSessionMessageFilter, LogMessages}
import org.apache.flink.yarn.YarnMessages._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

/** Actor which is responsible to repeatedly poll the Yarn cluster status from the ResourceManager.
  *
  * This class represents the bridge between the [[YarnClusterClient]] and the
  * [[YarnApplicationMasterRunner]].
  *
  * @param flinkConfig Configuration object
  * @param leaderRetrievalService [[LeaderRetrievalService]] which is used to retrieve the current
  *                              leading [[org.apache.flink.runtime.jobmanager.JobManager]]
  */
class ApplicationClient(
    val flinkConfig: Configuration,
    val leaderRetrievalService: LeaderRetrievalService)
  extends FlinkActor
  with LeaderSessionMessageFilter
  with LogMessages
  with LeaderRetrievalListener{

  val log = Logger(getClass)

  val INITIAL_POLLING_DELAY = 0 seconds
  val WAIT_FOR_YARN_INTERVAL = 2 seconds
  val POLLING_INTERVAL = 3 seconds

  var yarnJobManager: Option[ActorRef] = None
  var pollingTimer: Option[Cancellable] = None
  var running = false
  var messagesQueue : mutable.Queue[InfoMessage] = mutable.Queue[InfoMessage]()
  var stopMessageReceiver : Option[ActorRef] = None

  var leaderSessionID: Option[UUID] = None

  override def preStart(): Unit = {
    super.preStart()

    try {
      leaderRetrievalService.start(this)
    } catch {
      case e: Exception =>
        log.error("Could not start the leader retrieval service.", e)
        throw new RuntimeException("Could not start the leader retrieval service.", e)
    }
  }

  override def postStop(): Unit = {
    log.info("Stopped Application client.")

    disconnectFromJobManager()

    try {
      leaderRetrievalService.stop()
    } catch {
      case e: Exception => log.error("Leader retrieval service did not shout down properly.")
    }

    // Terminate the whole actor system because there is only the application client running
    context.system.shutdown()
  }

  override def handleMessage: Receive = {
    // ----------------------------- Registration -> Status updates -> shutdown ----------------

    case TriggerApplicationClientRegistration(jobManagerAkkaURL, currentTimeout, deadline) =>
      if (isConnected) {
        // we are already connected to the job manager
        log.debug("ApplicationClient is already registered to the " +
          s"JobManager ${yarnJobManager.get}.")
      } else {
        if (deadline.forall(_.isOverdue())) {
          // we failed to register in time. That means we should quit
          log.error(s"Failed to register at the JobManager with address $jobManagerAkkaURL. " +
            s"Shutting down...")

          self ! decorateMessage(PoisonPill)
        } else {
          log.info(s"Trying to register at JobManager $jobManagerAkkaURL.")

          val jobManager = context.actorSelection(jobManagerAkkaURL)

          jobManager ! decorateMessage(
            RegisterInfoMessageListener.getInstance()
          )

          val nextTimeout = (currentTimeout * 2).min(ApplicationClient.MAX_REGISTRATION_TIMEOUT)

          context.system.scheduler.scheduleOnce(
            currentTimeout,
            self,
            decorateMessage(
              TriggerApplicationClientRegistration(
                jobManagerAkkaURL,
                nextTimeout,
                deadline
              )
            )
          )(context.dispatcher)
        }
      }

    case msg: RegisterInfoMessageListenerSuccessful =>
      // The job manager acts as a proxy between the client and the resource manager
      val jm = sender()
      log.info(s"Successfully registered at the ResourceManager using JobManager $jm")
      yarnJobManager = Some(jm)

    case JobManagerLeaderAddress(jobManagerAkkaURL, newLeaderSessionID) =>
      log.info(s"Received address of new leader $jobManagerAkkaURL with session ID" +
        s" $newLeaderSessionID.")
      disconnectFromJobManager()

      leaderSessionID = Option(newLeaderSessionID)

      Option(jobManagerAkkaURL).foreach{
        akkaURL =>
          if (akkaURL.nonEmpty) {
            val maxRegistrationDuration = ApplicationClient.MAX_REGISTRATION_DURATION

            val deadline = if (maxRegistrationDuration.isFinite()) {
              Some(maxRegistrationDuration.fromNow)
            } else {
              None
            }

            // trigger registration at new leader
            self ! decorateMessage(
              TriggerApplicationClientRegistration(
                akkaURL,
                ApplicationClient.INITIAL_REGISTRATION_TIMEOUT,
                deadline))
          }
      }

    case msg @ LocalStopYarnSession(status, diagnostics) =>
      log.info("Sending StopCluster request to JobManager.")

      // preserve the original sender so we can reply
      val originalSender = sender()

      yarnJobManager match {
        case Some(jm) =>
          jm.tell(decorateMessage(new StopCluster(status, diagnostics)), originalSender)
        case None =>
          context.system.scheduler.scheduleOnce(1 second) {
            // try once more; we might have been connected in the meantime
            self.tell(msg, originalSender)
          }(context.dispatcher)
      }

    // -----------------  handle messages from the cluster -------------------
    // receive remote messages
    case msg: InfoMessage =>
      log.debug(s"Received new YarnMessage $msg. Now ${messagesQueue.size} messages in queue")
      messagesQueue.enqueue(msg)

    // locally forward messages
    case LocalGetYarnMessage =>
      if (messagesQueue.nonEmpty) {
        sender() ! decorateMessage(Option(messagesQueue.dequeue()))
      } else {
        sender() ! decorateMessage(None)
      }
  }

  /** Disconnects this [[ApplicationClient]] from the connected [[YarnJobManager]] and cancels
    * the polling timer.
    *
    */
  def disconnectFromJobManager(): Unit = {
    log.info(s"Disconnect from JobManager ${yarnJobManager.getOrElse(ActorRef.noSender)}.")

    yarnJobManager foreach {
      _ ! decorateMessage(UnRegisterInfoMessageListener.get())
    }

    pollingTimer foreach {
      _.cancel()
    }

    yarnJobManager = None
    leaderSessionID = None
    pollingTimer = None
  }

  /** True if the [[ApplicationClient]] is connected to the [[YarnJobManager]]
    *
    * @return true if the client is connected to the JobManager, otherwise false
    */
  def isConnected: Boolean = {
    yarnJobManager.isDefined
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }

  override def notifyLeaderAddress(leaderAddress: String, leaderSessionID: UUID): Unit = {
    log.info(s"Notification about new leader address $leaderAddress with " +
      s"session ID $leaderSessionID.")
    self ! JobManagerLeaderAddress(leaderAddress, leaderSessionID)
  }

  override def handleError(exception: Exception): Unit = {
    log.error("Error in leader retrieval service.", exception)

    // in case of an error in the LeaderRetrievalService, we shut down the ApplicationClient
    self ! decorateMessage(PoisonPill)
  }
}

object ApplicationClient {
  val INITIAL_REGISTRATION_TIMEOUT: FiniteDuration = 500 milliseconds
  val MAX_REGISTRATION_DURATION: FiniteDuration = 5 minutes
  val MAX_REGISTRATION_TIMEOUT = 5 minutes
}
