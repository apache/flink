package org.apache.flink.state.api.scala

import org.apache.flink.api.scala.DataSet
import org.apache.flink.state.api.OneInputOperatorTransformation

object OperatorTransformation {
  def bootstrapWith[T](dataSet: DataSet[T]) = {
    new OneInputOperatorTransformation[T](dataSet.javaSet)
  }
}
