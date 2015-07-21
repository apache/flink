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

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.CallingThreadDispatcher
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.jobmanager.{MemoryArchivist, JobManager}
import org.apache.flink.runtime.minicluster.FlinkMiniCluster
import org.apache.flink.runtime.net.NetUtils
import org.apache.flink.runtime.taskmanager.TaskManager

/**
 * Testing cluster which starts the [[JobManager]] and [[TaskManager]] actors with testing support
 * in the same [[ActorSystem]].
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true if all actors shall be running in the same [[ActorSystem]],
 *                          otherwise false
 */
class TestingCluster(userConfiguration: Configuration,
                     singleActorSystem: Boolean,
                     synchronousDispatcher: Boolean,
                     streamingMode: StreamingMode)
  extends FlinkMiniCluster(userConfiguration,
                           singleActorSystem,
                           streamingMode) {
  

  def this(userConfiguration: Configuration,
           singleActorSystem: Boolean,
           synchronousDispatcher: Boolean)
       = this(userConfiguration, singleActorSystem, synchronousDispatcher, StreamingMode.BATCH_ONLY)

  def this(userConfiguration: Configuration, singleActorSystem: Boolean)
       = this(userConfiguration, singleActorSystem, false)

  def this(userConfiguration: Configuration) = this(userConfiguration, true, false)
  
  // --------------------------------------------------------------------------
  
  override def generateConfiguration(userConfig: Configuration): Configuration = {
    val cfg = new Configuration()
    cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost")
    cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, NetUtils.getAvailablePort())
    cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 10)
    cfg.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, -1)

    cfg.addAll(userConfig)
    cfg
  }

  override def startJobManager(actorSystem: ActorSystem): ActorRef = {

    val (executionContext,
      instanceManager,
      scheduler,
      libraryCacheManager,
      _,
      executionRetries,
      delayBetweenRetries,
      timeout,
      archiveCount) = JobManager.createJobManagerComponents(configuration)
    
    val testArchiveProps = Props(new MemoryArchivist(archiveCount) with TestingMemoryArchivist)
    val archive = actorSystem.actorOf(testArchiveProps, JobManager.ARCHIVE_NAME)
    
    val jobManagerProps = Props(
      new JobManager(
        configuration,
        executionContext,
        instanceManager,
        scheduler,
        libraryCacheManager,
        archive,
        executionRetries,
        delayBetweenRetries,
        timeout,
        streamingMode)
      with TestingJobManager)

    val dispatcherJobManagerProps = if (synchronousDispatcher) {
      // disable asynchronous futures (e.g. accumulator update in Heartbeat)
      jobManagerProps.withDispatcher(CallingThreadDispatcher.Id)
    } else {
      jobManagerProps
    }

    actorSystem.actorOf(dispatcherJobManagerProps, JobManager.JOB_MANAGER_NAME)
  }

  override def startTaskManager(index: Int, system: ActorSystem) = {

    val tmActorName = TaskManager.TASK_MANAGER_NAME + "_" + (index + 1)

    val jobManagerPath: Option[String] = if (singleActorSystem) {
      Some(jobManagerActor.path.toString)
    } else {
      None
    }
    
    TaskManager.startTaskManagerComponentsAndActor(configuration, system,
                                                   hostname,
                                                   Some(tmActorName),
                                                   jobManagerPath,
                                                   numTaskManagers == 1,
                                                   streamingMode,
                                                   classOf[TestingTaskManager])
  }
}
