package org.apache.flink.mesos.runtime.clusterframework

import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.instance.InstanceConnectionInfo
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.taskmanager.{TaskManager, TaskManagerConfiguration}

/** An extension of the TaskManager that listens for additional Mesos-related
  * messages.
  */
class MesosTaskManager(
                       config: TaskManagerConfiguration,
                       resourceID: ResourceID,
                       connectionInfo: InstanceConnectionInfo,
                       memoryManager: MemoryManager,
                       ioManager: IOManager,
                       network: NetworkEnvironment,
                       numberOfSlots: Int,
                       leaderRetrievalService: LeaderRetrievalService)
  extends TaskManager(
    config,
    resourceID,
    connectionInfo,
    memoryManager,
    ioManager,
    network,
    numberOfSlots,
    leaderRetrievalService) {

  override def handleMessage: Receive = {
    super.handleMessage
  }
}

object MesosTaskManager {
  /** Entry point (main method) to run the TaskManager on Mesos.
    *
    * @param args The command line arguments.
    */
  def main(args: Array[String]): Unit = {
    MesosTaskManagerRunner.runTaskManager(args, classOf[MesosTaskManager])
  }

}
