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

import akka.actor._
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus
import org.apache.flink.yarn.Messages._
import scala.collection.mutable
import scala.concurrent.duration._

import scala.language.postfixOps
import scala.util.{Failure, Success}

class ApplicationClient extends Actor with ActorLogMessages with ActorLogging {
  import context._

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

    timeout = AkkaUtils.getTimeout(GlobalConfiguration.getConfiguration())
  }

  override def postStop(): Unit = {
    log.info("Stopped Application client.")
    pollingTimer foreach {
      _.cancel()
    }

    pollingTimer = None
  }

  override def receiveWithLogMessages: Receive = {
    // ----------------------------- Registration -> Status updates -> shutdown ----------------
    case LocalRegisterClient(address: String) =>
      val jmAkkaUrl = JobManager.getRemoteAkkaURL(address)

      val jobManagerFuture = AkkaUtils.getReference(jmAkkaUrl)(system, timeout)

      jobManagerFuture.onComplete {
        case Success(jm) => self ! JobManagerActorRef(jm)
        case Failure(t) =>
          log.error(t, "Registration at JobManager/ApplicationMaster failed. Shutting " +
            "ApplicationClient down.")
          self ! PoisonPill
      }

    case JobManagerActorRef(jm) =>
      yarnJobManager = Some(jm)

      // the message came from the FlinkYarnCluster. We send the message to the JobManager.
      // it is important not to forward the message because the JobManager is storing the
      // sender as the Application Client (this class).
      jm ! RegisterClient

      // schedule a periodic status report from the JobManager
      // request the number of task managers and slots from the job manager
      pollingTimer = Some(context.system.scheduler.schedule(INITIAL_POLLING_DELAY,
        WAIT_FOR_YARN_INTERVAL, jm, PollYarnClusterStatus))

    case msg: StopYarnSession =>
      log.info("Stop yarn session.")
      stopMessageReceiver = Some(sender())
      yarnJobManager foreach {
        _ forward msg
      }

    case JobManagerStopped =>
      log.info("Remote JobManager has been stopped successfully. " +
        "Stopping local application client")
      stopMessageReceiver foreach {
        _ ! JobManagerStopped
      }
      // stop ourselves
      context.system.shutdown()


    // handle the responses from the PollYarnClusterStatus messages to the yarn job mgr
    case status: FlinkYarnClusterStatus =>
      latestClusterStatus = Some(status)


    // locally get cluster status
    case LocalGetYarnClusterStatus =>
      sender() ! latestClusterStatus


    // -----------------  handle messages from the cluster -------------------
    // receive remote messages
    case msg: YarnMessage =>
      messagesQueue.enqueue(msg)

    // locally forward messages
    case LocalGetYarnMessage =>
      sender() ! messagesQueue.headOption
  }

}
