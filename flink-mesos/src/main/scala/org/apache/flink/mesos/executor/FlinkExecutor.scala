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

import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.mesos._
import org.apache.flink.mesos.scheduler._
import org.apache.flink.runtime.StreamingMode
import org.apache.log4j.{ConsoleAppender, Level, Logger => ApacheLogger, PatternLayout}
import org.apache.mesos.Protos._
import org.apache.mesos.{Executor, ExecutorDriver}

import scala.util.{Failure, Success, Try}

trait FlinkExecutor extends Executor {
  // logger to use
  def LOG: org.slf4j.Logger

  var currentRunningTaskId: Option[TaskID] = None
  val TASK_MANAGER_LOGGING_LEVEL_KEY = "taskmanager.logging.level"
  val DEFAULT_TASK_MANAGER_LOGGING_LEVEL = "INFO"


  // methods that defines how the task is started when a launchTask is sent
  def startTask(streamingMode: StreamingMode): Try[Unit]

  var thread: Option[Thread] = None
  //  var baseConfig: Configuration = GlobalConfiguration.getConfiguration
  var slaveId: Option[SlaveID] = None

  override def shutdown(driver: ExecutorDriver): Unit = {
    LOG.info("Killing taskManager thread")
    // kill task manager thread
    for (t <- thread) {
      t.stop()
    }

    // exit
    sys.exit(0)
  }

  override def disconnected(driver: ExecutorDriver): Unit = {}

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {
    LOG.info(s"Killing task : ${taskId.getValue}")
    if (thread.isEmpty) {
      return
    }

    thread.get.stop()
    thread = None
    currentRunningTaskId = None

    // Send the TASK_FINISHED status
    new Thread("TaskFinishedUpdate") {
      override def run(): Unit = {
        driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(taskId)
          .setState(TaskState.TASK_FINISHED)
          .build())
      }
    }.start()
  }


  override def error(driver: ExecutorDriver, message: String): Unit = {}

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {}

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo,
                          frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {
    LOG.info(s"${executorInfo.getName} was registered on slave: ${slaveInfo.getHostname}")
    slaveId = Some(slaveInfo.getId)
    // get the configuration passed to it
    if (executorInfo.hasData) {
      val newConfig: Configuration = Utils.deserialize(executorInfo.getData.toByteArray)
      GlobalConfiguration.includeConfiguration(newConfig)
    }
    LOG.debug("Loaded configuration: {}", GlobalConfiguration.getConfiguration)
  }


  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    slaveId = Some(slaveInfo.getId)
  }

  def initializeLog4j(level: Level): Unit = {
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
    ApacheLogger.getRootLogger.setLevel(level)
  }

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    // overlay the new config over this one
    if (task.hasData) {
      val taskConf: Configuration = Utils.deserialize(task.getData.toByteArray)
      GlobalConfiguration.includeConfiguration(taskConf)
      // reconfigure log4j
      val logLevel = GlobalConfiguration.getString(
        TASK_MANAGER_LOGGING_LEVEL_KEY, DEFAULT_TASK_MANAGER_LOGGING_LEVEL)
      initializeLog4j(Level.toLevel(logLevel, Level.DEBUG))
    }

    LOG.debug(s"Task configuration ${GlobalConfiguration.getConfiguration.toMap}")

    // get streaming mode
    val streamingMode =
      GlobalConfiguration.getString(STREAMING_MODE_KEY, DEFAULT_STREAMING_MODE) match {
        case "streaming" => StreamingMode.STREAMING
        case _ => StreamingMode.BATCH_ONLY
      }

    // create the thread
    val t = new Thread {
      override def run(): Unit = {
        startTask(streamingMode) match {
          case Failure(throwable) =>
            LOG.error("Caught exception, committing suicide.", throwable)
            driver.stop()
            sys.exit(1)
          case Success(_) =>
            // Send a TASK_FINISHED status update.
            // We do this here because we want to send it in a separate thread
            // than was used to call killTask().
            LOG.info("TaskManager finished running for task: {}", task.getTaskId.getValue)
            driver.sendStatusUpdate(TaskStatus.newBuilder()
              .setTaskId(task.getTaskId)
              .setState(TaskState.TASK_FINISHED)
              .build())
            // Stop the executor.
            driver.stop()
        }
      }
    }
    t.start()
    thread = Some(t)

    // send message
    driver.sendStatusUpdate(TaskStatus.newBuilder()
      .setTaskId(task.getTaskId)
      .setState(TaskState.TASK_RUNNING)
      .build())
  }
}
