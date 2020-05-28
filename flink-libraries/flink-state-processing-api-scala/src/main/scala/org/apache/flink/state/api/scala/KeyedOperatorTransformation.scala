package org.apache.flink.state.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.state.api.{SavepointWriterOperatorFactory, KeyedOperatorTransformation => JKeyedOperatorTransformation}

class KeyedOperatorTransformation[K: TypeInformation, T](keyedOperatorTransformation: JKeyedOperatorTransformation[K, T]) {
  def transform(processFunction: KeyedStateBootstrapFunction[K, T]): BootstrapTransformation[T] = {
    val bootstrap = keyedOperatorTransformation.transform(processFunction)
    asScalaBootstrapTransformation(bootstrap)
  }

  def transform(factory: SavepointWriterOperatorFactory): BootstrapTransformation[T] = {
    val bootstrap = keyedOperatorTransformation.transform(factory)
    asScalaBootstrapTransformation(bootstrap)
  }
}
