package org.apache.flink.mesos

import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.runtime.StreamingMode
import org.apache.mesos.Protos._
import org.apache.mesos.{Executor, ExecutorDriver}
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

// represents the state of a task
case class TaskThreadConfiguration(taskInfo: TaskInfo, thread: Thread)

trait FlinkExecutor extends Executor {
  // logger to use
  def LOG: Logger

  // methods that defines how the task is started when a launchTask is sent
  def startTask(conf: Configuration, streamingMode: StreamingMode): Try[Unit]

  var baseConfig: Option[Configuration] = None
  var runningTasks: Map[TaskID, TaskThreadConfiguration] = Map()

  override def shutdown(driver: ExecutorDriver): Unit = {
    LOG.info("Shutdown received")
  }

  override def disconnected(driver: ExecutorDriver): Unit = {
    LOG.info("Disconnected from mesos slave")
  }

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {
    if (runningTasks.contains(taskId)) {
      val task = runningTasks(taskId)
      if (task.thread.isAlive) {
        task.thread.interrupt()
      }
    }

    // remove this task
    runningTasks -= taskId

    val state = TaskStatus.newBuilder()
      .setTaskId(taskId)
      .setState(TaskState.TASK_KILLED)
      .build()
    driver.sendStatusUpdate(state)
  }


  override def error(driver: ExecutorDriver, message: String): Unit = {
    LOG.info("error received: {}", message)
  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {}

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {
    LOG.info("{} was registered on slave: {}", executorInfo.getName, slaveInfo.getHostname)

    // get the configuration passed to it
    if (executorInfo.hasData) {
      baseConfig = Some(deserialize(executorInfo.getData.toByteArray))
    } else {
      baseConfig = None
    }

    LOG.debug("Loaded configuration: {}", baseConfig.get)
  }


  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    LOG.info("Re-registered on slave: {}", slaveInfo.getHostname)
    // TODO: do some state reconciliation (if applicable)
  }

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {

    // load configuration
    sendStatus(driver, task.getTaskId, TaskState.TASK_STARTING, Some("Loading configuration"))

    if (baseConfig.isEmpty) {
      GlobalConfiguration.loadConfiguration("confDir")
      baseConfig = Some(GlobalConfiguration.getConfiguration)
    }

    // create a copy of the base configuration
    val conf = baseConfig.get.clone()

    // overlay the new config over this one
    if (task.hasData) {
      val taskConf = deserialize(task.getData.toByteArray)
      conf.addAll(taskConf)
    }

    GlobalConfiguration.includeConfiguration(conf)

    // get streaming mode
    val streamingMode = GlobalConfiguration.getString("streamingMode", "batch").toLowerCase match {
      case "streaming" => StreamingMode.STREAMING
      case _ => StreamingMode.BATCH_ONLY
    }

    // run the TaskManager
    sendStatus(driver, task.getTaskId, TaskState.TASK_STARTING, Some("Starting Task"))

    // create the TaskManager thread
    val t = new Thread {
      override def run(): Unit = {
        startTask(conf, streamingMode) match {
          case Failure(throwable) =>
            // something bad happened
            LOG.error("Failed to run TaskManager.", throwable)
            // send status message
            sendStatus(driver, task.getTaskId, TaskState.TASK_FAILED, Some(s"Unable to run TaskManager ${throwable.getCause}"))
            // remove this task from the map of running tasks
            runningTasks -= task.getTaskId
          case Success(_) =>
            LOG.info("TaskManager finished running for task: {}", task.getTaskId.getValue)
            sendStatus(driver, task.getTaskId, TaskState.TASK_FINISHED)
        }
      }
    }
    // start the thread
    t.run()
    // add to map of running tasks
    runningTasks += (task.getTaskId -> new TaskThreadConfiguration(task, t))

    // send update
    sendStatus(driver, task.getTaskId, TaskState.TASK_RUNNING)
  }

  def sendStatus(driver: ExecutorDriver, taskId: TaskID, taskState: TaskState, message: Option[String] = None): Unit = {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
      .setTaskId(taskId)
      .setState(taskState)
      .setMessage(message.getOrElse(""))
      .build())
  }
}
