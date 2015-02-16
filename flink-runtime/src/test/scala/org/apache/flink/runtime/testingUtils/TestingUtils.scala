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

package org.apache.flink.runtime.testingUtils

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.testkit.CallingThreadDispatcher
import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.ActionQueue
import org.apache.flink.runtime.jobmanager.{MemoryArchivist, JobManager}
import org.apache.flink.runtime.taskmanager.TaskManager
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

/**
 * Convenience functions to test actor based components.
 */
object TestingUtils {

  val testConfig = ConfigFactory.parseString(getDefaultTestingActorSystemConfigString)

  val TESTING_DURATION = 2 minute

  val DEFAULT_AKKA_ASK_TIMEOUT = "200 s"

  def getDefaultTestingActorSystemConfigString: String = {
    val logLevel = AkkaUtils.getLogLevel

    s"""akka.daemonic = on
      |akka.test.timefactor = 10
      |akka.loggers = ["akka.event.slf4j.Slf4jLogger"]
      |akka.loglevel = $logLevel
      |akka.stdout-loglevel = WARNING
      |akka.jvm-exit-on-fata-error = off
      |akka.log-config-on-start = off
    """.stripMargin
  }

  def getDefaultTestingActorSystemConfig = testConfig

  def startTestingTaskManagerWithConfiguration(hostname: String, jobManagerURL: String,
                                               config: Configuration)
                                              (implicit system: ActorSystem) = {
    val (connectionInfo, _, taskManagerConfig, networkConnectionConfig) =
      TaskManager.parseConfiguration(hostname, config,
        localAkkaCommunication = true, localTaskManagerCommunication = false)
    system.actorOf(Props(new TaskManager(connectionInfo, jobManagerURL, taskManagerConfig,
      networkConnectionConfig) with TestingTaskManager))
  }

  def startTestingJobManager(implicit system: ActorSystem): ActorRef = {
    val config = new Configuration()

    val (instanceManager, scheduler, libraryCacheManager, _, accumulatorManager, _ ,
        executionRetries, delayBetweenRetries,
        timeout, archiveCount) = JobManager.createJobManagerComponents(config)

    val testArchiveProps = Props(new MemoryArchivist(archiveCount) with TestingMemoryArchivist)
    val archive = system.actorOf(testArchiveProps, JobManager.ARCHIVE_NAME)

    val jobManagerProps = Props(new JobManager(config, instanceManager, scheduler,
      libraryCacheManager, archive, accumulatorManager, None, executionRetries,
      delayBetweenRetries, timeout) with TestingJobManager)

    system.actorOf(jobManagerProps, JobManager.JOB_MANAGER_NAME)
  }

  def startTestingTaskManager(jobManager: ActorRef)(implicit system: ActorSystem): ActorRef = {
    val jmURL = jobManager.path.toString
    val config = new Configuration()
    val (connectionInfo, _, taskManagerConfig, networkConnectionConfig) =
      TaskManager.parseConfiguration("localhost", config,
        localAkkaCommunication = true, localTaskManagerCommunication = true)

    system.actorOf(Props(new TaskManager(connectionInfo, jmURL, taskManagerConfig,
      networkConnectionConfig) with TestingTaskManager))
  }

  def startTestingCluster(numSlots: Int, numTMs: Int = 1,
                          timeout: String = DEFAULT_AKKA_ASK_TIMEOUT):
  TestingCluster = {
    val config = new Configuration()
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTMs)
    config.setInteger(ConfigConstants.JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT_KEY, 1000)
    config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, timeout)
    new TestingCluster(config)
  }

  def startTestingClusterDeathWatch(numSlots: Int, numTMs: Int,
                                            timeout: String = DEFAULT_AKKA_ASK_TIMEOUT):
  TestingCluster = {
    val config = new Configuration()
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTMs)
    config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, timeout)
    config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, "200 ms")
    config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, "50 ms")
    config.setDouble(ConfigConstants.AKKA_WATCH_THRESHOLD, 1)

    new TestingCluster(config, singleActorSystem = false)
  }

  def setGlobalExecutionContext(): Unit = {
    AkkaUtils.globalExecutionContext = ExecutionContext.global
  }

  def setCallingThreadDispatcher(system: ActorSystem): Unit = {
    AkkaUtils.globalExecutionContext = system.dispatchers.lookup(CallingThreadDispatcher.Id)
  }

  def setExecutionContext(context: ExecutionContext): Unit = {
    AkkaUtils.globalExecutionContext = context
  }

  class QueuedActionExecutionContext(queue: ActionQueue) extends ExecutionContext {
    var automaticExecution = false

    def toggleAutomaticExecution() = {
      automaticExecution = !automaticExecution
    }

    override def execute(runnable: Runnable): Unit = {
      if(automaticExecution){
        runnable.run()
      }else {
        queue.queueAction(runnable)
      }
    }

    override def reportFailure(t: Throwable): Unit = {
      t.printStackTrace()
    }
  }
}
