package org.apache.flink.state.api.scala

import java.util.OptionalInt

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.core.fs.Path
import org.apache.flink.state.api.{BootstrapTransformation, SavepointWriterOperatorFactory}
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState
import org.apache.flink.state.api.output.operators.KeyedStateBootstrapOperator
import org.apache.flink.streaming.api.operators.StreamOperator

class KeyedOperatorTransformation[K: TypeInformation, T](dataSet: DataSet[T],
                                        operatorMaxParallelism: OptionalInt,
                                        keySelector: KeySelector[T, K]) {
  def transform(processFunction: KeyedStateBootstrapFunction[K, T]): BootstrapTransformation[T] = {
    val factory = new SavepointWriterOperatorFactory {
      override def createOperator(savepointTimestamp: Long, savepointPath: Path): StreamOperator[TaggedOperatorSubtaskState] = new KeyedStateBootstrapOperator(savepointTimestamp, savepointPath, processFunction)
    }
    transform(factory)
  }

  def transform(factory: SavepointWriterOperatorFactory): BootstrapTransformation[T] = {
    val keyType: TypeInformation[K] = implicitly[TypeInformation[K]]

    new BootstrapTransformation(dataSet.javaSet, operatorMaxParallelism, factory, keySelector, keyType)
  }
}
