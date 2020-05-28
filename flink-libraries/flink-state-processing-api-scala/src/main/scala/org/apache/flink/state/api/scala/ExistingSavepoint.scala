package org.apache.flink.state.api.scala

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.state.api.WritableSavepoint
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata

class ExistingSavepoint(env: ExecutionEnvironment, metadata: SavepointMetadata, stateBackend: StateBackend)
  extends WritableSavepoint(metadata, stateBackend) {

}
