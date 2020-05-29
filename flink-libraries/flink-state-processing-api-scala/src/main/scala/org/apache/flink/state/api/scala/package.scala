/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
