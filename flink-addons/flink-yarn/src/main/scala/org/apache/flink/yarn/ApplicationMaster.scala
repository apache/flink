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

import java.io.{PrintWriter, FileWriter, BufferedWriter}
import java.security.PrivilegedAction

import akka.actor._
import org.apache.flink.client.CliFrontend
import org.apache.flink.configuration.{GlobalConfiguration, ConfigConstants}
import org.apache.flink.runtime.jobmanager.{WithWebServer, JobManager}
import org.apache.flink.yarn.Messages.StartYarnSession
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.slf4j.LoggerFactory

import scala.io.Source

object ApplicationMaster{
  import scala.collection.JavaConversions._

  val LOG = LoggerFactory.getLogger(this.getClass)

  val CONF_FILE = "flink-conf.yaml"
  val MODIFIED_CONF_FILE = "flink-conf-modified.yaml"

  def main(args: Array[String]): Unit ={
    val yarnClientUsername = System.getenv(Client.ENV_CLIENT_USERNAME)
    LOG.info(s"YARN daemon runs as ${UserGroupInformation.getCurrentUser.getShortUserName} " +
      s"' setting user to execute Flink ApplicationMaster/JobManager to ${yarnClientUsername}'")

    val ugi = UserGroupInformation.createRemoteUser(yarnClientUsername)

    for(token <- UserGroupInformation.getCurrentUser.getTokens){
      ugi.addToken(token)
    }

    ugi.doAs(new PrivilegedAction[Object] {
      override def run(): Object = {
        var actorSystem: ActorSystem = null
        var jobManager: ActorRef = ActorRef.noSender

        try {
          val conf = Utils.initializeYarnConfiguration()

          val env = System.getenv()

          val currDir = env.get(Environment.PWD.key())
          require(currDir != null, "Current directory unknown.")

          val logDirs = env.get(Environment.LOG_DIRS.key())

          val ownHostname = env.get(Environment.NM_HOST.key())
          require(ownHostname != null, s"Own hostname not set.")

          val taskManagerCount = env.get(Client.ENV_TM_COUNT).toInt
          val slots = env.get(Client.ENV_SLOTS).toInt
          val dynamicPropertiesEncodedString = env.get(Client.ENV_DYNAMIC_PROPERTIES)

          val appNumber = env.get(Client.ENV_APP_NUMBER).toInt

          val jobManagerPort = GlobalConfiguration.getInteger(
            ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
            ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT) match {
            case x if x <= 0 => x
            case x => x + appNumber
          }

          val jobManagerWebPort = GlobalConfiguration.getInteger(ConfigConstants
            .JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT)

          generateConfigurationFile(currDir, ownHostname, jobManagerPort, jobManagerWebPort,
            logDirs, slots, taskManagerCount, dynamicPropertiesEncodedString)

          val (system, actor) = startJobManager(currDir)

          actorSystem = system
          jobManager = actor

          LOG.info("Start yarn session on job manager.")
          jobManager ! StartYarnSession(conf)

          LOG.info("Await termination of actor system.")
          actorSystem.awaitTermination()
        }catch{
          case t: Throwable =>
            LOG.error("Error while running the application master.", t)

            if(actorSystem != null){
              actorSystem.shutdown()
              actorSystem.awaitTermination()

              actorSystem = null
            }
        }

        null
      }
    })

  }

  def generateConfigurationFile(currDir: String, ownHostname: String, jobManagerPort: Int,
                               jobManagerWebPort: Int, logDirs: String, slots: Int,
                               taskManagerCount: Int, dynamicPropertiesEncodedString: String)
  : Unit = {
    LOG.info("Generate configuration file for application master.")
    val output = new PrintWriter(new BufferedWriter(
      new FileWriter(s"$currDir/$MODIFIED_CONF_FILE"))
    )

    for (line <- Source.fromFile(s"$currDir/$CONF_FILE").getLines() if !(line.contains
      (ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY))) {
      output.println(line)
    }

    output.println(s"${ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY}: $ownHostname")
    output.println(s"${ConfigConstants.JOB_MANAGER_IPC_PORT_KEY}: $jobManagerPort")
    output.println(s"${ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY}: $logDirs")
    output.println(s"${ConfigConstants.JOB_MANAGER_WEB_PORT_KEY}: $jobManagerWebPort")

    if(slots != -1){
      output.println(s"${ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS}: $slots")
      output.println(
        s"${ConfigConstants.DEFAULT_PARALLELIZATION_DEGREE_KEY}: ${slots*taskManagerCount}")
    }

    // add dynamic properties
    val dynamicProperties = CliFrontend.getDynamicProperties(dynamicPropertiesEncodedString)

    import scala.collection.JavaConverters._

    for(property <- dynamicProperties.asScala){
      output.println(s"${property.f0}: ${property.f1}")
    }

    output.close()
  }

  def startJobManager(currDir: String): (ActorSystem, ActorRef) = {
    LOG.info("Start job manager for yarn")
    val pathToConfig = s"$currDir/$MODIFIED_CONF_FILE"
    val args = Array[String]("--configDir", pathToConfig)

    LOG.info(s"Config path: ${pathToConfig}.")
    val (hostname, port, configuration, _) = JobManager.parseArgs(args)

    implicit val jobManagerSystem = YarnUtils.createActorSystem(hostname, port, configuration)

    LOG.info("Start job manager actor.")
    (jobManagerSystem, JobManager.startActor(Props(new JobManager(configuration) with
      WithWebServer with YarnJobManager)))
  }
}
