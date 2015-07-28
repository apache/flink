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

import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.log4j.{Logger => ApacheLogger}
import org.apache.mesos.MesosExecutorDriver
import org.apache.mesos.Protos.Status
import org.slf4j.{Logger, LoggerFactory}

import scala.tools.nsc.io
import scala.util.Try

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

  def checkEnvironment(): Unit = {
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

  def apply(args: Array[String]): TaskManagerExecutor = {
    // initialize sandbox
    // create a tmp data directory
    io.File("tmpData").createDirectory(force = true, failIfExists = false)

    // create executor
    new TaskManagerExecutor
  }

  def main(args: Array[String]) {

    // initialize sandbox
    // create a tmp data directory
    io.File("tmpData").createDirectory(force = true, failIfExists = false)

    // create dummy config dir
    val configDirPath = io.File("configDir")
      .createDirectory(force = true, failIfExists = false)
      .path
    GlobalConfiguration.loadConfiguration(configDirPath)

    // create executor
    val tmExecutor = new TaskManagerExecutor

    // start the executor
    val driver = new MesosExecutorDriver(tmExecutor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    sys.exit(status)
  }

}
