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
import grizzled.slf4j.Logger
import org.apache.flink.client.CliFrontend
import org.apache.flink.configuration.{GlobalConfiguration, Configuration, ConfigConstants}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.instance.AkkaActorGateway
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.jobmanager.web.WebInfoServer
import org.apache.flink.runtime.util.{StandaloneUtils, LeaderRetrievalUtils, EnvironmentInformation}
import org.apache.flink.runtime.webmonitor.WebMonitor
import org.apache.flink.yarn.Messages.StartYarnSession
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration


import scala.io.Source

object ApplicationMaster {
  import scala.collection.JavaConversions._

  val LOG = Logger(getClass)

  val CONF_FILE = "flink-conf.yaml"
  val MODIFIED_CONF_FILE = "flink-conf-modified.yaml"
  val MAX_REGISTRATION_DURATION = "5 minutes"

  def main(args: Array[String]): Unit = {

    val yarnClientUsername = System.getenv(FlinkYarnClient.ENV_CLIENT_USERNAME)
    LOG.info(s"YARN daemon runs as ${UserGroupInformation.getCurrentUser.getShortUserName} " +
      s"setting user to execute Flink ApplicationMaster/JobManager to ${yarnClientUsername}")

    EnvironmentInformation.logEnvironmentInfo(LOG.logger, "YARN ApplicationMaster/JobManager", args)
    EnvironmentInformation.checkJavaVersion()
    org.apache.flink.runtime.util.SignalHandler.register(LOG.logger)
    
    var streamingMode = StreamingMode.BATCH_ONLY

    val ugi = UserGroupInformation.createRemoteUser(yarnClientUsername)

    for(token <- UserGroupInformation.getCurrentUser.getTokens){
      ugi.addToken(token)
    }

    ugi.doAs(new PrivilegedAction[Object] {
      override def run(): Object = {

        var actorSystem: ActorSystem = null
        var webserver: WebMonitor = null

        try {
          val conf = new YarnConfiguration()

          val env = System.getenv()

          if (LOG.isDebugEnabled) {
            LOG.debug("All environment variables: " + env.toString)
          }

          val currDir = env.get(Environment.PWD.key())
          require(currDir != null, "Current directory unknown.")

          val logDirs = env.get(Environment.LOG_DIRS.key())

          if(hasStreamingMode(env)) {
            LOG.info("Starting ApplicationMaster/JobManager in streaming mode")
            streamingMode = StreamingMode.STREAMING
          }

          // Note that we use the "ownHostname" given by YARN here, to make sure
          // we use the hostnames given by YARN consistently throughout akka.
          // for akka "localhost" and "localhost.localdomain" are different actors.
          val ownHostname = env.get(Environment.NM_HOST.key())
          require(ownHostname != null, "Own hostname in YARN not set.")

          val taskManagerCount = env.get(FlinkYarnClient.ENV_TM_COUNT).toInt
          val slots = env.get(FlinkYarnClient.ENV_SLOTS).toInt
          val dynamicPropertiesEncodedString = env.get(FlinkYarnClient.ENV_DYNAMIC_PROPERTIES)

          val config = createConfiguration(currDir, dynamicPropertiesEncodedString)

          val (
            system: ActorSystem,
            jobManager: ActorRef,
            archiver: ActorRef) = startJobManager(
              config,
              ownHostname,
              streamingMode)

          actorSystem = system
          val address = AkkaUtils.getAddress(actorSystem)
          val jobManagerPort = address.port.get

          if (config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) != -1) {
            // start the web info server
            LOG.info("Starting Job Manger web frontend.")
            config.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, logDirs)
            config.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0); // set port to 0.
            // set JobManager host/port for web interface.
            config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, ownHostname)
            config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort)

            // TODO: Add support for HA: Make web server work independently from the JM
            val leaderRetrievalService = StandaloneUtils.createLeaderRetrievalService(config)

            webserver = if(
              config.getBoolean(
                ConfigConstants.JOB_MANAGER_NEW_WEB_FRONTEND_KEY,
                false)) {
              JobManager.startWebRuntimeMonitor(config, leaderRetrievalService, actorSystem)
            } else {
              new WebInfoServer(config, leaderRetrievalService, actorSystem)
            }

            webserver.start()
          }

          val jobManagerWebPort = if (webserver == null) {
            LOG.warn("Web server is null. It will not be accessible through YARN")
            -1
          } else webserver.getServerPort

          // generate configuration file for TaskManagers
          generateConfigurationFile(s"$currDir/$MODIFIED_CONF_FILE", currDir, ownHostname,
            jobManagerPort, jobManagerWebPort, logDirs, slots, taskManagerCount,
            dynamicPropertiesEncodedString)

          // send "start yarn session" message to YarnJobManager.
          LOG.info("Starting YARN session on Job Manager.")
          jobManager ! StartYarnSession(conf, jobManagerPort, jobManagerWebPort)

          LOG.info("Application Master properly initiated. Awaiting termination of actor system.")
          actorSystem.awaitTermination()
        }
        catch {
          case t: Throwable =>
            LOG.error("Error while running the application master.", t)

            if (actorSystem != null) {
              actorSystem.shutdown()
              actorSystem.awaitTermination()
            }
        }
        finally {
          if (webserver != null) {
            LOG.debug("Stopping Job Manager web frontend.")
            webserver.stop()
          }
        }

        null
      }
    })

  }

  def generateConfigurationFile(
      fileName: String,
      currDir: String,
      ownHostname: String,
      jobManagerPort: Int,
      jobManagerWebPort: Int,
      logDirs: String,
      slots: Int,
      taskManagerCount: Int,
      dynamicPropertiesEncodedString: String)
    : Unit = {
    LOG.info("Generate configuration file for application master.")
    val output = new PrintWriter(new BufferedWriter(
      new FileWriter(fileName))
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
        s"${ConfigConstants.DEFAULT_PARALLELISM_KEY}: ${slots*taskManagerCount}")
    }

    output.println(s"${ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION}: " +
      s"$MAX_REGISTRATION_DURATION")

    // add dynamic properties
    val dynamicProperties = CliFrontend.getDynamicProperties(dynamicPropertiesEncodedString)

    import scala.collection.JavaConverters._

    for(property <- dynamicProperties.asScala){
      output.println(s"${property.f0}: ${property.f1}")
    }

    output.close()
  }

  /**
   * Starts the JobManager and all its components.
   *
   * @return (Configuration, JobManager ActorSystem, JobManager ActorRef, Archiver ActorRef)
   */
  def startJobManager(
      configuration: Configuration,
      hostname: String,
      streamingMode: StreamingMode)
    : (ActorSystem, ActorRef, ActorRef) = {

    LOG.info("Starting JobManager for YARN")

    // set port to 0 to let Akka automatically determine the port.
    LOG.debug("Starting JobManager actor system")
    val jobManagerSystem = AkkaUtils.createActorSystem(configuration, Some((hostname, 0)))

    // start all the components inside the job manager
    LOG.debug("Starting JobManager components")
    val (executionContext,
      instanceManager,
      scheduler,
      libraryCacheManager,
      archiveProps,
      executionRetries,
      delayBetweenRetries,
      timeout,
      _,
      leaderElectionService) = JobManager.createJobManagerComponents(configuration)

    // start the archiver
    val archiver: ActorRef = jobManagerSystem.actorOf(archiveProps, JobManager.ARCHIVE_NAME)

    val jobManagerProps = Props(
      new JobManager(
        configuration,
        executionContext,
        instanceManager,
        scheduler,
        libraryCacheManager,
        archiver,
        executionRetries,
        delayBetweenRetries,
        timeout,
        streamingMode,
        leaderElectionService)
      with ApplicationMasterActor)

    LOG.debug("Starting JobManager actor")
    val jobManager = JobManager.startActor(jobManagerProps, jobManagerSystem)

    (jobManagerSystem, jobManager, archiver)
  }

  def createConfiguration(curDir: String, dynamicPropertiesEncodedString: String): Configuration = {
    LOG.info(s"Loading config from: $curDir.")

    GlobalConfiguration.loadConfiguration(curDir)
    val configuration = GlobalConfiguration.getConfiguration()

    configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, curDir)

    // add dynamic properties to JobManager configuration.
    val dynamicProperties = CliFrontend.getDynamicProperties(dynamicPropertiesEncodedString)
    import scala.collection.JavaConverters._
    for(property <- dynamicProperties.asScala){
      configuration.setString(property.f0, property.f1)
    }

    configuration
  }


  def hasStreamingMode(env: java.util.Map[String, String]): Boolean = {
    val sModeString = env.get(FlinkYarnClient.ENV_STREAMING_MODE)
    if(sModeString != null) {
      return sModeString.toBoolean
    }
    false
  }
}
