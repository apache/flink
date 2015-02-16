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
import org.apache.flink.configuration.{Configuration, ConfigConstants}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.jobmanager.web.WebInfoServer
import org.apache.flink.yarn.Messages.StartYarnSession
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory

import scala.io.Source

object ApplicationMaster {
  import scala.collection.JavaConversions._

  val LOG = LoggerFactory.getLogger(this.getClass)

  val CONF_FILE = "flink-conf.yaml"
  val MODIFIED_CONF_FILE = "flink-conf-modified.yaml"
  val MAX_REGISTRATION_DURATION = "5 minutes"

  def main(args: Array[String]): Unit ={
    val yarnClientUsername = System.getenv(FlinkYarnClient.ENV_CLIENT_USERNAME)
    LOG.info(s"YARN daemon runs as ${UserGroupInformation.getCurrentUser.getShortUserName}" +
      s"' setting user to execute Flink ApplicationMaster/JobManager to $yarnClientUsername'")

    val ugi = UserGroupInformation.createRemoteUser(yarnClientUsername)

    for(token <- UserGroupInformation.getCurrentUser.getTokens){
      ugi.addToken(token)
    }

    ugi.doAs(new PrivilegedAction[Object] {
      override def run(): Object = {

        var actorSystem: ActorSystem = null
        var webserver: WebInfoServer = null

        try {
          val conf = new YarnConfiguration()

          val env = System.getenv()

          if (LOG.isDebugEnabled) {
            LOG.debug("All environment variables: " + env.toString)
          }

          val currDir = env.get(Environment.PWD.key())
          require(currDir != null, "Current directory unknown.")

          val logDirs = env.get(Environment.LOG_DIRS.key())

          // Note that we use the "ownHostname" given by YARN here, to make sure
          // we use the hostnames given by YARN consistently throughout akka.
          // for akka "localhost" and "localhost.localdomain" are different actors.
          val ownHostname = env.get(Environment.NM_HOST.key())
          require(ownHostname != null, "Own hostname in YARN not set.")

          val taskManagerCount = env.get(FlinkYarnClient.ENV_TM_COUNT).toInt
          val slots = env.get(FlinkYarnClient.ENV_SLOTS).toInt
          val dynamicPropertiesEncodedString = env.get(FlinkYarnClient.ENV_DYNAMIC_PROPERTIES)

          val (config, system, jobManager, archiver) = startJobManager(currDir, ownHostname,
                                                      dynamicPropertiesEncodedString, logDirs)

          actorSystem = system
          val extActor = system.asInstanceOf[ExtendedActorSystem]
          val jobManagerPort = extActor.provider.getDefaultAddress.port.get

          // start the web info server
          LOG.info("Starting Job Manger web frontend.")
          webserver = new WebInfoServer(config, jobManager, archiver)

          val jobManagerWebPort = webserver.getServer.getConnectors()(0).getLocalPort

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

  def generateConfigurationFile(fileName: String, currDir: String, ownHostname: String,
                               jobManagerPort: Int,
                               jobManagerWebPort: Int, logDirs: String, slots: Int,
                               taskManagerCount: Int, dynamicPropertiesEncodedString: String)
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
        s"${ConfigConstants.DEFAULT_PARALLELIZATION_DEGREE_KEY}: ${slots*taskManagerCount}")
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
   * @param currDir
   * @param hostname
   * @param dynamicPropertiesEncodedString
   * @param logDirs
   *
   * @return (Configuration, JobManager ActorSystem, JobManager ActorRef, Archiver ActorRef)
   */
  def startJobManager(currDir: String,
                      hostname: String,
                      dynamicPropertiesEncodedString: String,
                      logDirs: String): (Configuration, ActorSystem, ActorRef, ActorRef) = {

    LOG.info("Starting JobManager for YARN")
    val args = Array[String]("--configDir", currDir)

    LOG.info(s"Config path: $currDir.")
    val (configuration, _, _) = JobManager.parseArgs(args)

    // add dynamic properties to JobManager configuration.
    val dynamicProperties = CliFrontend.getDynamicProperties(dynamicPropertiesEncodedString)
    import scala.collection.JavaConverters._
    for(property <- dynamicProperties.asScala){
      configuration.setString(property.f0, property.f1)
    }

    // set port to 0 to let Akka automatically determine the port.
    LOG.debug("Starting JobManager actor system")
    val jobManagerSystem = AkkaUtils.createActorSystem(configuration, Some((hostname, 0)))

    // start all the components inside the job manager
    LOG.debug("Starting JobManager components")
    val (instanceManager, scheduler, libraryCacheManager, archiveProps, accumulatorManager,
                   profilerProps, executionRetries, delayBetweenRetries,
                   timeout, _) = JobManager.createJobManagerComponents(configuration)

    // start the profiler, if needed
    val profiler: Option[ActorRef] =
      profilerProps.map( props => jobManagerSystem.actorOf(props, JobManager.PROFILER_NAME) )

    // start the archiver
    val archiver: ActorRef = jobManagerSystem.actorOf(archiveProps, JobManager.ARCHIVE_NAME)

    val jobManagerProps = Props(new JobManager(configuration, instanceManager, scheduler,
      libraryCacheManager, archiver, accumulatorManager, profiler, executionRetries,
      delayBetweenRetries, timeout) with YarnJobManager)

    LOG.debug("Starting JobManager actor")
    val jobManager = JobManager.startActor(jobManagerProps, jobManagerSystem)

    (configuration, jobManagerSystem, jobManager, archiver)
  }
}
