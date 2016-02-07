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

package org.apache.flink.mesos.executor

import scala.util.Try

import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver}
import org.apache.mesos.Protos.Status
import org.slf4j.{Logger, LoggerFactory}

/**
 * A simple executor for managing the lifecycle and configuration of the
 * Apache Flink TaskManager process.
 */
class TaskManagerExecutor extends FlinkExecutor {

  def LOG: Logger = LoggerFactory.getLogger(classOf[TaskManagerExecutor])

  // methods that defines how the task is started when a launchTask is sent
  override def startTask(streamingMode: StreamingMode): Try[Unit] = {
    // startup checks
    checkEnvironment()

    // start TaskManager
    Try(TaskManager.selectNetworkInterfaceAndRunTaskManager(
      GlobalConfiguration.getConfiguration, streamingMode, classOf[TaskManager]))
  }

  private def checkEnvironment(): Unit = {
    EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManagerExecutor", null)
    EnvironmentInformation.checkJavaVersion()
    val maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit
    if (maxOpenFileHandles != -1) {
      LOG.info(s"Maximum number of open file descriptors is $maxOpenFileHandles")
    } else {
      LOG.info("Cannot determine the maximum number of open file descriptors")
    }
  }

}

object TaskManagerExecutor {
  val LOG = LoggerFactory.getLogger(classOf[TaskManagerExecutor])

  def createExecutor(): Executor = {
    new TaskManagerExecutor()
  }
  def createDriver(executor: Executor): ExecutorDriver = {
    new MesosExecutorDriver(executor)
  }

  def main(args: Array[String]) {
    GlobalConfiguration.loadConfiguration(".")

    // create executor
    val tmExecutor = createExecutor()

    // start the executor
    val driver = createDriver(tmExecutor)

    // exit based on result of run
    sys.exit(if (driver.run eq Status.DRIVER_STOPPED) 0 else 1)
  }

}
