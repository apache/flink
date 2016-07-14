package org.apache.flink.mesos.runtime.clusterframework

import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore
import org.apache.flink.runtime.clusterframework.types.{ResourceID, ResourceIDRetrievable}

/**
  * A representation of a registered Mesos task managed by the {@link MesosFlinkResourceManager}.
  */
case class RegisteredMesosWorkerNode(task: MesosWorkerStore.Worker) extends ResourceIDRetrievable {

  require(task.slaveID().isDefined)
  require(task.hostname().isDefined)

  override val getResourceID: ResourceID = MesosFlinkResourceManager.extractResourceID(task.taskID())
}
