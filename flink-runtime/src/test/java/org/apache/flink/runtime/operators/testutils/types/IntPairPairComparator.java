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


import java.net.InetSocketAddress

import akka.actor._
import akka.pattern.ask
import org.apache.flink.configuration.{GlobalConfiguration, ConfigConstants, Configuration}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.execution.ExecutionState2
import org.apache.flink.runtime.execution.librarycache.LibraryCacheUpdate
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.instance.{HardwareDescription, InstanceID}
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager
import org.apache.flink.runtime.messages.RegistrationMessages.{RegisterTaskManager, AcknowledgeRegistration}
import org.apache.flink.runtime.messages.TaskManagerMessages.{AcknowledgeLibraryCacheUpdate, SendHeartbeat, Heartbeat, RegisterAtMaster}
import org.apache.flink.runtime.net.NetUtils
import org.apache.flink.runtime.util.EnvironmentInformation
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Failure

class TaskManager(jobManagerURL: String, numberOfSlots: Int, memorySize: Long,
                  pageSize: Int) extends Actor with ActorLogMessages with
ActorLogging {
  import context.dispatcher
  import AkkaUtils.FUTURE_TIMEOUT

  val REGISTRATION_DELAY = 0 seconds
  val REGISTRATION_INTERVAL = 10 seconds
  val MAX_REGISTRATION_ATTEMPTS = 1
  val HEARTBEAT_INTERVAL = 200 millisecond


  val memoryManager = new DefaultMemoryManager(memorySize, numberOfSlots, pageSize)

  val hardwareDescription = HardwareDescription.extractFromSystem(memoryManager.getMemorySize)

  var registrationScheduler: Option[Cancellable] = None
  var registrationAttempts: Int = 0
  var registered: Boolean = false
  var currentJobManager = ActorRef.noSender
  var instanceID: InstanceID = null;

  override def preStart(): Unit = {
    tryJobManagerRegistration()
  }

  def tryJobManagerRegistration(): Unit = {
    registrationAttempts = 0
    import context.dispatcher
    registrationScheduler = Some(context.system.scheduler.schedule(REGISTRATION_DELAY, REGISTRATION_INTERVAL,
      self, RegisterAtMaster))
  }


  override def receiveWithLogMessages: Receive = {
    case RegisterAtMaster =>
      registrationAttempts += 1

      if(registered){
        registrationScheduler.foreach(_.cancel())
      } else if(registrationAttempts <= MAX_REGISTRATION_ATTEMPTS){
        val jobManagerURL = getJobManagerURL

        log.info(s"Try to register at master ${jobManagerURL}. ${registrationAttempts}. Attempt")
        val jobManager = context.actorSelection(jobManagerURL)

        jobManager ! RegisterTaskManager(hardwareDescription, numberOfSlots)
      }else{
        log.error("TaskManager could not register at JobManager.");
        throw new RuntimeException("TaskManager could not register at JobManager");
      }

    case AcknowledgeRegistration(id) =>
      registered = true
      currentJobManager = sender()
      instanceID = id
      val jobManagerAddress = currentJobManager.path.toString()
      log.info(s"TaskManager successfully registered at JobManager $jobManagerAddress.")
      context.system.scheduler.schedule(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL, self, SendHeartbeat)

    case SendHeartbeat =>
      currentJobManager ! Heartbeat(instanceID)

    case x:LibraryCacheUpdate =>
      log.info(s"Registered library ${x.getLibraryFileName}.")
      sender() ! AcknowledgeLibraryCacheUpdate

  }

  def notifyExecutionStateChange(jobID: JobID, executionID: ExecutionAttemptID, executionState : ExecutionState2,
                                 message: String): Unit = {
    val futureResponse = currentJobManager ? new TaskExecutionState(jobID, executionID, executionState, message)

    futureResponse.onComplete{
      x =>
        x match {
          case Failure(ex) =>
            log.error(ex, "Error sending task state update to JobManager.")
          case _ =>
        }
        if(executionState == ExecutionState2.FINISHED || executionState == ExecutionState2.CANCELED || executionState ==
          ExecutionState2.FAILED){
          unregisterTask(executionID)
        }
    }
  }

  def unregisterTask(executionID: ExecutionAttemptID): Unit = {

  }

  private def getJobManagerURL: String = {
    JobManager.getAkkaURL(jobManagerURL)
  }
}

public class IntPairPairComparator extends TypePairComparator<IntPair, IntPair> {
	
	private int key;
	

  val LOG = LoggerFactory.getLogger(classOf[TaskManager])
  val FAILURE_RETURN_CODE = -1

  def main(args: Array[String]): Unit = {
    val (hostname, port, configuration) = initialize(args)

    val taskManagerSystem = startActorSystemAndActor(hostname, port, configuration)
    taskManagerSystem.awaitTermination()
  }

  private def initialize(args: Array[String]):(String, Int, Configuration) = {
    val parser = new scopt.OptionParser[TaskManagerCLIConfiguration]("taskmanager"){
      head("flink task manager")
      opt[String]("configDir") action { (x, c) =>
        c.copy(configDir = x)
      } text("Specify configuration directory.")
      opt[String]("tempDir") action { (x, c) =>
        c.copy(tmpDir = x)
      } text("Specify temporary directory.")
    }


    parser.parse(args, TaskManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)

        val configuration = GlobalConfiguration.getConfiguration()

        if(config.tmpDir != null && GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
          null) == null){
          configuration.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, config.tmpDir)
        }

        val jobManagerHostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
        val jobManagerPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        val jobManagerAddress = new InetSocketAddress(jobManagerHostname, jobManagerPort);

        val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0)
        val hostname = NetUtils.resolveAddress(jobManagerAddress).getHostName;

        (hostname, port, configuration)
    } getOrElse {
      LOG.error("CLI parsing failed. Usage: " + parser.usage)
      sys.exit(FAILURE_RETURN_CODE)
    }
  }

  def startActorSystemAndActor(hostname: String, port: Int, configuration: Configuration) = {
    val actorSystem = AkkaUtils.createActorSystem(hostname, port, configuration)
    startActor(actorSystem, configuration)
    actorSystem
  }

  def startActor(actorSystem: ActorSystem, configuration: Configuration): ActorRef = {
    val jobManagerAddress = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
    val jobManagerRPCPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

    if(jobManagerAddress == null){
      throw new RuntimeException("JobManager address has not been specified in the configuration.")
    }

    val jobManagerURL = jobManagerAddress + ":" + jobManagerRPCPort
    val slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)

    val numberOfSlots = if(slots > 0) slots else 1

    val configuredMemory:Long = configuration.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1)

    val memorySize = if(configuredMemory > 0){
      configuredMemory << 20
    } else{
      val fraction = configuration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)
      (EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag * fraction).toLong
    }

    val pageSize = configuration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE)

    actorSystem.actorOf(Props(classOf[TaskManager], jobManagerURL, numberOfSlots, memorySize, pageSize), "taskmanager");
  }

  def getAkkaURL(address: String): String = {
    s"akka.tcp://flink@${address}/user/taskmanager"
  }
}
