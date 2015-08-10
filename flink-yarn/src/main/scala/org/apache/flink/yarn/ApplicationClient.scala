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

import java.net.InetSocketAddress

import akka.actor._
import akka.pattern.ask
import grizzled.slf4j.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.{FlinkActor, LogMessages}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus
import org.apache.flink.yarn.Messages._
import scala.collection.mutable
import scala.concurrent.duration._

import scala.language.postfixOps
import scala.util.{Failure, Success}

class ApplicationClient(flinkConfig: Configuration)
  extends FlinkActor with LogMessages {
  import context._

  val log = Logger(getClass)

  val INITIAL_POLLING_DELAY = 0 seconds
  val WAIT_FOR_YARN_INTERVAL = 2 seconds
  val POLLING_INTERVAL = 3 seconds

  var yarnJobManager: Option[ActorRef] = None
  var pollingTimer: Option[Cancellable] = None
  implicit var timeout: FiniteDuration = 0 seconds
  var running = false
  var messagesQueue : mutable.Queue[YarnMessage] = mutable.Queue[YarnMessage]()
  var latestClusterStatus : Option[FlinkYarnClusterStatus] = None
  var stopMessageReceiver : Option[ActorRef] = None

  override def preStart(): Unit = {
    super.preStart()

    timeout = AkkaUtils.getTimeout(flinkConfig)
  }

  override def postStop(): Unit = {
    log.info("Stopped Application client.")
    pollingTimer foreach {
      _.cancel()
    }

    pollingTimer = None

    // Terminate the whole actor system because there is only the application client running
    context.system.shutdown()
  }

  override def handleMessage: Receive = {
    // ----------------------------- Registration -> Status updates -> shutdown ----------------
    case LocalRegisterClient(address: InetSocketAddress) =>
      val jmAkkaUrl = JobManager.getRemoteJobManagerAkkaURL(address)

      val jobManagerFuture = AkkaUtils.getReference(jmAkkaUrl, system, timeout)

      jobManagerFuture.onComplete {
        case Success(jm) => self ! decorateMessage(JobManagerActorRef(jm))
        case Failure(t) =>
          log.error("Registration at JobManager/ApplicationMaster failed. Shutting " +
            "ApplicationClient down.", t)

          // we could not connect to the job manager --> poison ourselves
          self ! decorateMessage(PoisonPill)
      }

    case JobManagerActorRef(jm) =>
      yarnJobManager = Some(jm)

      // the message came from the FlinkYarnCluster. We send the message to the JobManager.
      // it is important not to forward the message because the JobManager is storing the
      // sender as the Application Client (this class).
      (jm ? decorateMessage(RegisterClient(self)))(timeout).onFailure{
        case t: Throwable =>
          log.error("Could not register at the job manager.", t)
          self ! decorateMessage(PoisonPill)
      }

      // schedule a periodic status report from the JobManager
      // request the number of task managers and slots from the job manager
      pollingTimer = Some(
        context.system.scheduler.schedule(
          INITIAL_POLLING_DELAY,
          WAIT_FOR_YARN_INTERVAL,
          jm,
          PollYarnClusterStatus)
      )

    case LocalUnregisterClient =>
      // unregister client from AM
      yarnJobManager foreach {
        _ ! decorateMessage(UnregisterClient)
      }
      // poison ourselves
      self ! decorateMessage(PoisonPill)

    case msg: StopYarnSession =>
      log.info("Sending StopYarnSession request to ApplicationMaster.")
      stopMessageReceiver = Some(sender)
      yarnJobManager foreach {
        _ forward decorateMessage(msg)
      }

    case JobManagerStopped =>
      log.info("Remote JobManager has been stopped successfully. " +
        "Stopping local application client")
      stopMessageReceiver foreach {
        _ ! decorateMessage(JobManagerStopped)
      }
      // poison ourselves
      self ! decorateMessage(PoisonPill)

    // handle the responses from the PollYarnClusterStatus messages to the yarn job mgr
    case status: FlinkYarnClusterStatus =>
      latestClusterStatus = Some(status)


    // locally get cluster status
    case LocalGetYarnClusterStatus =>
      sender() ! decorateMessage(latestClusterStatus)

    // Forward message to Application Master
    case msg: StopAMAfterJob =>
      yarnJobManager foreach {
        _ forward decorateMessage(msg)
      }

    // -----------------  handle messages from the cluster -------------------
    // receive remote messages
    case msg: YarnMessage =>
      log.debug(s"Received new YarnMessage $msg. Now ${messagesQueue.size} messages in queue")
      messagesQueue.enqueue(msg)

    // locally forward messages
    case LocalGetYarnMessage =>
      if(messagesQueue.size > 0) {
        sender() ! decorateMessage(Option(messagesQueue.dequeue))
      } else {
        sender() ! decorateMessage(None)
      }
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }
}
