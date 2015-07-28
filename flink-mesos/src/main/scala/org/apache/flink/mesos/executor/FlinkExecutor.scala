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

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.mesos._
import org.apache.flink.runtime.StreamingMode
import org.apache.mesos.Protos._
import org.apache.mesos.{Executor, ExecutorDriver}
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

trait FlinkExecutor extends Executor {
  // logger to use
  def LOG: Logger

  var currentRunningTaskId: Option[TaskID] = None
  val pool = Executors.newScheduledThreadPool(1)

  // methods that defines how the task is started when a launchTask is sent
  def startTask(streamingMode: StreamingMode): Try[Unit]

  var thread: Option[Thread] = None
  //  var baseConfig: Configuration = GlobalConfiguration.getConfiguration
  var slaveId: Option[SlaveID] = None

  override def shutdown(driver: ExecutorDriver): Unit = {
    LOG.info("Killing taskManager thread")
    // kill task manager thread
    for (t <- thread) {
      t.interrupt()
    }

    // shutdown ping pool
    LOG.info("shutting down executor ping")
    pool.shutdown()

    // exit
    sys.exit(0)
  }

  override def disconnected(driver: ExecutorDriver): Unit = {}

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {
    LOG.info(s"Killing task : ${taskId.getValue}")
    if (thread.isEmpty) {
      return
    }

    thread.get.interrupt()
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

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    // overlay the new config over this one
    if (task.hasData) {
      val taskConf: Configuration = Utils.deserialize(task.getData.toByteArray)
      GlobalConfiguration.includeConfiguration(taskConf)
    }

    // get streaming mode
    val streamingMode = GlobalConfiguration.getString("streamingMode", "batch").toLowerCase match {
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
            pool.shutdown()
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

    // schedule periodic heartbeat pings to the framework scheduler
    pool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        for (
        // create a ping only if running a task and registered
          taskId <- currentRunningTaskId;
          slave <- slaveId) {
          // send ping
          val ping = ExecutorPing(taskId, slave)
          val data = Utils.serialize(ping)
          LOG.info(s"Sending ping to scheduler: $ping")
          driver.sendFrameworkMessage(data)
        }
      }
    }, 60, 10, TimeUnit.SECONDS)
  }
}
