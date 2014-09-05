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


import java.io.File
import java.net.InetAddress

import akka.actor._
import org.apache.flink.configuration.{ConfigConstants, GlobalConfiguration, Configuration}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.executiongraph.{ExecutionGraph}
import org.apache.flink.runtime.instance.{InstanceManager}
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.jobmanager.accumulators.AccumulatorManager
import org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler
import org.apache.flink.runtime.messages.EventCollectorMessages.RegisterArchiveListener
import org.apache.flink.runtime.messages.JobManagerMessages.{CancelJob, SubmitJob, RequestNumberRegisteredTaskManager}
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages.Heartbeat
import org.apache.flink.runtime.profiling.ProfilingUtils
import org.apache.flink.runtime.profiling.impl.JobManagerProfilerImpl
import org.slf4j.LoggerFactory

class JobManager(archiveCount: Int, profiling: Boolean, recommendedPollingInterval: Int) extends Actor with
ActorLogMessages with ActorLogging {

  val profiler = if(profiling){
    new JobManagerProfilerImpl(InetAddress.getByName(self.path.address.host.getOrElse("localhost")))
  }else{
    null
  }

  // will be removed
  val archive = context.actorOf(Props(classOf[MemoryArchivist], archiveCount), "archive")
  val eventCollector = context.actorOf(Props(classOf[EventCollector], recommendedPollingInterval), "eventcollector")


  val accumulatorManager = new AccumulatorManager(Math.min(1, archiveCount))
  val instanceManager = new InstanceManager()
  val scheduler = new DefaultScheduler()
  val webserver = null

  val currentJobs = scala.collection.concurrent.TrieMap[JobID, ExecutionGraph]()

  eventCollector ! RegisterArchiveListener(archive)

  instanceManager.addInstanceListener(scheduler)

  override def postStop(): Unit = {
    instanceManager.shutdown()
    scheduler.shutdown()
  }

  override def receiveWithLogMessages: Receive = {
    case RegisterTaskManager(hardwareInformation, numberOfSlots) =>
      val taskManager = sender()
      val instanceID  = instanceManager.registerTaskManager(taskManager, hardwareInformation, numberOfSlots)
      context.watch(taskManager);
      taskManager ! AcknowledgeRegistration(instanceID)

    case RequestNumberRegisteredTaskManager =>
      sender() ! instanceManager.getNumberOfRegisteredTaskManagers

    case SubmitJob(jobGraph) =>

    case CancelJob(jobID) =>

    case Heartbeat(instanceID) =>
      instanceManager.reportHeartBeat(instanceID)


  }
}

object JobManager{
  val LOG = LoggerFactory.getLogger(classOf[JobManager])
  val FAILURE_RETURN_CODE = 1

  def main(args: Array[String]):Unit = {
    val (hostname, port, configuration) = initialize(args)

    val jobManagerSystem = startActorSystemAndActor(hostname, port, configuration)
    jobManagerSystem.awaitTermination()
  }

  def initialize(args: Array[String]):(String, Int, Configuration) = {
    val parser = new scopt.OptionParser[JobManagerCLIConfiguration]("jobmanager"){
      head("flink jobmanager")
      opt[String]("configDir") action { (x, c) =>
        c.copy(configDir = x)
      } text("Specify configuration directory.")
    }

    parser.parse(args, JobManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)
        val configuration = GlobalConfiguration.getConfiguration()
        if(config.configDir != null && new File(config.configDir).isDirectory){
          configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, config.configDir + "/..")
        }

        val hostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)
        val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        (hostname, port, configuration)
    } getOrElse {
      LOG.error("CLI Parsing failed. Usage: " + parser.usage)
      sys.exit(FAILURE_RETURN_CODE)
    }
  }

  def startActorSystemAndActor(hostname: String, port: Int, configuration: Configuration): ActorSystem = {
    val actorSystem = AkkaUtils.createActorSystem(hostname, port, configuration)
    startActor(actorSystem, configuration)
    actorSystem
  }

  def startActor(actorSystem: ActorSystem, configuration: Configuration): ActorRef = {
    val archiveCount = configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT,
      ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT)
    val profilingEnabled = configuration.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)
    val recommendedPollingInterval = configuration.getInteger(ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY,
      ConfigConstants.DEFAULT_JOBCLIENT_POLLING_INTERVAL)

    actorSystem.actorOf(Props(classOf[JobManager], archiveCount, profilingEnabled, recommendedPollingInterval), "jobmanager")
  }

  def getAkkaURL(address: String): String = {
    s"akka.tcp://flink@${address}/user/jobmanager"
  }
}
