package org.apache.flink.mesos.executor

import java.util.concurrent.{TimeUnit, Executors}

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
  var slaveInfo: Option[SlaveInfo] = None

  override def shutdown(driver: ExecutorDriver): Unit = {
    pool.shutdown()
    LOG.info("Shutdown received")
  }

  override def disconnected(driver: ExecutorDriver): Unit = {
    LOG.info("Disconnected from mesos slave")
  }

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {
    LOG.info(s"Killing task : ${taskId.getValue}")
    if(thread.isEmpty) {
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


  override def error(driver: ExecutorDriver, message: String): Unit = {
    LOG.info("FlinkExecutor.error: {}", message)
  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {
    LOG.info(s"Executor received framework message of length: ${data.length} bytes")
  }

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {
    LOG.info(s"${executorInfo.getName} was registered on slave: ${slaveInfo.getHostname}")

    // get the configuration passed to it
    if (executorInfo.hasData) {
      val newConfig: Configuration = deserialize(executorInfo.getData.toByteArray)
      GlobalConfiguration.includeConfiguration(newConfig)
    }
    LOG.debug("Loaded configuration: {}", GlobalConfiguration.getConfiguration)
  }


  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    LOG.info("Re-registered on slave: {}", slaveInfo.getHostname)
  }

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    // overlay the new config over this one
    if (task.hasData) {
      val taskConf: Configuration = deserialize(task.getData.toByteArray)
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
        // as long as they are running we want to send out a ping
        if (currentRunningTaskId.isDefined) {
          driver.sendFrameworkMessage(currentRunningTaskId.get.toByteArray)
        }
      }
    }, 60, 10, TimeUnit.SECONDS)
  }

  def sendStatus(driver: ExecutorDriver, taskId: TaskID, taskState: TaskState, message: Option[String] = None): Unit = {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
      .setTaskId(taskId)
      .setState(taskState)
      .setMessage(message.getOrElse(""))
      .build())
  }
}
