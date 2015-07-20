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
package org.apache.flink.mesos

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.mesos.MesosExecutorDriver
import org.apache.mesos.Protos._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

class TaskManagerExecutor extends FlinkExecutor {

  def LOG: Logger = LoggerFactory.getLogger(classOf[TaskManagerExecutor])

  // methods that defines how the task is started when a launchTask is sent
  override def startTask(conf: Configuration, streamingMode: StreamingMode): Try[Unit] = {
    // start the TaskManager
    Try(TaskManager.selectNetworkInterfaceAndRunTaskManager(conf, streamingMode, classOf[TaskManager]))
  }

}

object TaskManagerExecutor {
  val LOG = LoggerFactory.getLogger(classOf[TaskManagerExecutor])

  def checkEnvironment(args: Array[String]): Unit = {
    EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManagerExecutor", args)
    EnvironmentInformation.checkJavaVersion()
    val maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit
    if (maxOpenFileHandles != -1) {
      LOG.info(s"Maximum number of open file descriptors is $maxOpenFileHandles")
    } else {
      LOG.info("Cannot determine the maximum number of open file descriptors")
    }
  }

  def main(args: Array[String]) {
    // load libmesos.so
    System.loadLibrary("mesos")

    // startup checks
    checkEnvironment(args)

    // start executor and get exit status
    val exitStatus = new MesosExecutorDriver(new TaskManagerExecutor).run()

    if (exitStatus == Status.DRIVER_STOPPED) {
      sys.exit(0)
    } else {
      sys.exit(1)
    }
  }

}
