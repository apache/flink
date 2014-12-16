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

import java.io.{File, FileOutputStream}
import java.util.Properties

import akka.actor._
import akka.camel.{Consumer, CamelMessage}
import org.apache.flink.client.CliFrontend
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.yarn.Messages._
import org.apache.hadoop.yarn.api.records.{FinalApplicationStatus, YarnApplicationState,
ApplicationId}
import org.apache.hadoop.yarn.client.api.YarnClient
import scala.concurrent.duration._

class ApplicationClient(appId: ApplicationId, port: Int, yarnClient: YarnClient,
                        confDirPath: String, slots: Int, numTaskManagers: Int,
                        dynamicPropertiesEncoded: String, timeout: FiniteDuration)
  extends Actor with Consumer with ActorLogMessages with ActorLogging {
  import context._

  val INITIAL_POLLING_DELAY = 0 seconds
  val WAIT_FOR_YARN_INTERVAL = 500 milliseconds
  val POLLING_INTERVAL = 3 seconds

  val waitingChars = Array[Char]('/', '|', '\\', '-')

  var jobManager: Option[ActorRef] = None
  var pollingTimer: Option[Cancellable] = None
  var running = false
  var waitingCharsIndex = 0

  def endpointUri = "stream:in"

  override def preStart(): Unit = {
    super.preStart()
    pollingTimer = Some(context.system.scheduler.schedule(INITIAL_POLLING_DELAY,
      WAIT_FOR_YARN_INTERVAL, self, PollYarnReport))
  }

  override def postStop(): Unit = {
    log.info("Stopped Application client.")
    pollingTimer foreach {
      _.cancel()
    }

    pollingTimer = None
  }

  override def receiveWithLogMessages: Receive = {
    case PollYarnReport => {
      val report = yarnClient.getApplicationReport(appId)

      report.getYarnApplicationState match {
        case YarnApplicationState.FINISHED | YarnApplicationState.KILLED | YarnApplicationState
          .FAILED => {
          log.info(s"Terminate polling.")

          context.system.shutdown()
        }
        case YarnApplicationState.RUNNING if !running => {
          val address = s"${report.getHost}:$port"
          log.info(s"Flink JobManager is now running on $address")
          log.info(s"JobManager Web Interface: ${report.getTrackingUrl}")

          writeYarnProperties(address)

          jobManager = Some(AkkaUtils.getReference(JobManager.getRemoteAkkaURL(address))(system,
            timeout))
          jobManager.get ! RegisterMessageListener

          pollingTimer foreach {
            _.cancel()
          }

          pollingTimer = Some(context.system.scheduler.schedule(INITIAL_POLLING_DELAY,
            POLLING_INTERVAL, self, PollYarnReport))

          running = true
        }
        case _ =>
      }

      if(!running){
        print(waitingChars(waitingCharsIndex) + "\r")
        waitingCharsIndex += 1

        if(waitingCharsIndex >= waitingChars.length){
          waitingCharsIndex = 0
        }
      }
    }
    case msg: YarnMessage => {
      println(msg)
    }
    case msg: StopYarnSession => {
      log.info("Stop yarn session.")
      jobManager foreach {
        _ forward msg
      }
    }
    case msg: CamelMessage => {
      msg.bodyAs[String] match {
        case "stop" | "quit" | "exit" => self ! StopYarnSession(FinalApplicationStatus.KILLED)
        case "help" => printHelp
        case msg => println(s"Unknown command ${msg}.")
      }
    }
  }

  def printHelp: Unit = {
    println(
      """Available commands:
        |stop : Stop the YARN session
      """.stripMargin)
  }

  def writeYarnProperties(address: String): Unit = {
    val yarnProps = new Properties()
    yarnProps.setProperty(CliFrontend.YARN_PROPERTIES_JOBMANAGER_KEY, address)

    if(slots > 0){
      yarnProps.setProperty(CliFrontend.YARN_PROPERTIES_DOP, (slots * numTaskManagers).toString )
    }

    if(dynamicPropertiesEncoded != null){
      yarnProps.setProperty(CliFrontend.YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING,
        dynamicPropertiesEncoded)
    }

    val yarnPropertiesFile = new File(confDirPath + CliFrontend.YARN_PROPERTIES_FILE)

    val out = new FileOutputStream(yarnPropertiesFile)
    yarnProps.store(out, "Generated YARN properties file")
    out.close()
    yarnPropertiesFile.setReadable(true, false)
  }
}
