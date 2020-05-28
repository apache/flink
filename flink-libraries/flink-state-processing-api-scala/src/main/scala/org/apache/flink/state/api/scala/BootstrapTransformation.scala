package org.apache.flink.state.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.MapPartitionOperator
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.checkpoint.OperatorState
import org.apache.flink.state.api.{BootstrapTransformation => JavaBootstrapTransformation}
import org.apache.flink.runtime.jobgraph.OperatorID
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState
import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.operators.StreamOperator

class BootstrapTransformation[T: TypeInformation](bootstrapTransformation: JavaBootstrapTransformation[T]) {

  def getMaxParallelism(globalMaxParallelism: Int): Int = bootstrapTransformation.getMaxParallelism(globalMaxParallelism)

  def writeOperatorState(operatorID: OperatorID, stateBackend: StateBackend, globalMaxParallelism: Int, savepointPath: Path): DataSet[OperatorState] =
    bootstrapTransformation.writeOperatorState(operatorID, stateBackend, globalMaxParallelism, savepointPath)

  def writeOperatorSubtaskStates(operatorID: OperatorID,
                                 stateBackend: StateBackend,
                                 savepointPath: Path,
                                 localMaxParallelism: Int): MapPartitionOperator[T, TaggedOperatorSubtaskState] =
    bootstrapTransformation.writeOperatorSubtaskStates(operatorID, stateBackend, savepointPath, localMaxParallelism)

  def getConfig(operatorID: OperatorID, stateBackend: StateBackend, operator: StreamOperator[TaggedOperatorSubtaskState]): StreamConfig =
    bootstrapTransformation.getConfig(operatorID, stateBackend, operator)

}
