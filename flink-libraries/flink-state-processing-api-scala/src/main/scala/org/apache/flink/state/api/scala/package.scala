package org.apache.flink.state.api

import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.state.api.{ExistingSavepoint => JExistingSavepoint}
import org.apache.flink.state.api.{NewSavepoint => JNewSavepoint}
import org.apache.flink.state.api.{BootstrapTransformation => JBootstrapTransformation, KeyedOperatorTransformation => JKeyedOperatorTransformation, OneInputOperatorTransformation => JOneInputOperatorTransformation}

package object scala {

  private[flink] def asScalaDataSet[T](dataSet: JDataSet[T]) = new DataSet[T](dataSet)

  private[flink] def asScalaBootstrapTransformation[T](bootstrapTransformation: JBootstrapTransformation[T]) = new BootstrapTransformation[T](bootstrapTransformation)

  private[flink] def asScalaOneInputOperatorTransformation[T](oneInputOperatorTransformation: JOneInputOperatorTransformation[T]) = new OneInputOperatorTransformation[T](oneInputOperatorTransformation)

  private[flink] def asScalaKeyedOperatorTransformation[K, V](keyedOperatorTransformation: JKeyedOperatorTransformation[K, V]) = new KeyedOperatorTransformation[K, V](keyedOperatorTransformation)

  private[flink] def asScalaExistingSavepoint(existingSavepoint: JExistingSavepoint) = new ExistingSavepoint(existingSavepoint)

  private[flink] def asScalaNewSavepoint(newSavepoint: JNewSavepoint) = new NewSavepoint(newSavepoint)
}
