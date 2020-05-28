package org.apache.flink.state.api

import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.DataSet

package object scala {
//  implicit def createTypeInformation[T]: TypeInformation[T] = macro TypeUtils.createTypeInfo[T]

  private[flink] def asScalaDataSet[T] (dataSet: JDataSet[T]) = new DataSet[T](dataSet)
}
