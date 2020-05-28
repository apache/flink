package org.apache.flink.state.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.state.api.functions.{BroadcastStateBootstrapFunction, StateBootstrapFunction}
import org.apache.flink.state.api.{SavepointWriterOperatorFactory, OneInputOperatorTransformation => JOneInputOperatorTransformation}

class OneInputOperatorTransformation[T](oneInputOperatorTransformation: JOneInputOperatorTransformation[T]) {
  def setMaxParallelism(maxParallelism: Int): OneInputOperatorTransformation[T] = {
    val result = oneInputOperatorTransformation.setMaxParallelism(maxParallelism)
    asScalaOneInputOperatorTransformation(result)
  }

  def transform(processFunction: StateBootstrapFunction[T]): BootstrapTransformation[T] = {
    val bootstrapTransformation = oneInputOperatorTransformation.transform(processFunction)
    asScalaBootstrapTransformation(bootstrapTransformation)
  }

  def transform(broadcastFunction: BroadcastStateBootstrapFunction[T]): BootstrapTransformation[T] = {
    val bootstrapTransformation = oneInputOperatorTransformation.transform(broadcastFunction)
    asScalaBootstrapTransformation(bootstrapTransformation)
  }

  def transform(factory: SavepointWriterOperatorFactory): BootstrapTransformation[T] = {
    val bootstrapTransformation = oneInputOperatorTransformation.transform(factory)
    asScalaBootstrapTransformation(bootstrapTransformation)
  }

  def keyBy[K](keySelector: KeySelector[T, K]): KeyedOperatorTransformation[K, T] = {
    val keyedOperatorTransformation = oneInputOperatorTransformation.keyBy(keySelector, implicitly[TypeInformation[K]])
    asScalaKeyedOperatorTransformation(keyedOperatorTransformation)
  }

  def keyBy(fields: Int*): KeyedOperatorTransformation[Tuple, T] = {
    val keyedOperatorTransformation = oneInputOperatorTransformation.keyBy(fields: _*)
    asScalaKeyedOperatorTransformation(keyedOperatorTransformation)
  }

  def keyBy(fields: String*): KeyedOperatorTransformation[Tuple, T] = {
    val keyedOperatorTransformation = oneInputOperatorTransformation.keyBy(fields: _*)
    asScalaKeyedOperatorTransformation(keyedOperatorTransformation)
  }
}
