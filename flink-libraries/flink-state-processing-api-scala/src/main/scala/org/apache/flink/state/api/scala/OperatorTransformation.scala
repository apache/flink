package org.apache.flink.state.api.scala

import org.apache.flink.api.scala.DataSet
import org.apache.flink.state.api.{OneInputOperatorTransformation, OperatorTransformation => JOperatorTransformation}

object OperatorTransformation {
  def bootstrapWith[T](dataSet: DataSet[T]): OneInputOperatorTransformation[T] = {
    JOperatorTransformation.bootstrapWith(dataSet.javaSet)
  }
}
