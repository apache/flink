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
import org.apache.log4j.{Logger => ApacheLogger, _}
import org.apache.mesos.MesosExecutorDriver
import org.apache.mesos.Protos.Status
import org.slf4j.{Logger, LoggerFactory}

import scala.tools.nsc.io
import scala.util.Try

class TaskManagerExecutor extends FlinkExecutor {

  def LOG: Logger = LoggerFactory.getLogger(classOf[TaskManagerExecutor])

  // methods that defines how the task is started when a launchTask is sent
  override def startTask(streamingMode: StreamingMode): Try[Unit] = {
    // start the TaskManager
    val conf = GlobalConfiguration.getConfiguration
    // get the logging level
    val level = Level.toLevel(conf.getString("taskmanager.logging.level", "INFO"), Level.INFO)
    // reconfigure log4j
    initializeLog4j(level)
    // start TaskManager
    Try(TaskManager.selectNetworkInterfaceAndRunTaskManager(conf, streamingMode, classOf[TaskManager]))
  }

  private def initializeLog4j(level: Level): Unit = {
    // remove all existing loggers
    ApacheLogger.getRootLogger.removeAllAppenders()

    // create a console appender
    val consoleAppender = new ConsoleAppender()
    consoleAppender.setLayout(new PatternLayout("%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n"))
    consoleAppender.setThreshold(level)
    consoleAppender.activateOptions()
    // reconfigure log4j
    ApacheLogger.getLogger("org.jboss.netty.channel.DefaultChannelPipeline").setLevel(Level.ERROR)
    ApacheLogger.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(Level.OFF)
    ApacheLogger.getRootLogger.addAppender(consoleAppender)
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

  def apply(args: Array[String]): TaskManagerExecutor = {
    // startup checks
    checkEnvironment(args)

    // initialize sandbox
    // create a tmp data directory
    io.File("tmpData").createDirectory(force = true, failIfExists = false)

    // create executor
    new TaskManagerExecutor
  }

  def main(args: Array[String]) {
    // start executor and get exit status
    val tmExecutor = TaskManagerExecutor(args)

    // start the executor
    val driver = new MesosExecutorDriver(tmExecutor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    sys.exit(status)
  }

}
