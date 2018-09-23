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
package org.apache.flink.api.scala

import org.apache.flink.annotation.{Internal, Public, PublicEvolving}
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.java.io.SplitDataProperties
import org.apache.flink.api.java.operators.{DataSource => JavaDataSource}
import org.apache.flink.configuration.Configuration

import scala.reflect.ClassTag

/**
  * An operation that creates a new data set (data source). The operation acts as the
  * data set on which to apply further transformations. It encapsulates additional
  * configuration parameters, to customize the execution.
  *
  * @tparam T The type of the elements produced by this data source.
  */
@Public
class DataSource[T: ClassTag](dataSource: JavaDataSource[T]) extends DataSet(dataSource) {

  @PublicEvolving
  def getSplitDataProperties(): SplitDataProperties[_] = dataSource.getSplitDataProperties()

  @Internal
  def getInputFormat(): InputFormat[_, _] = dataSource.getInputFormat()

  def getParameters(): Configuration = dataSource.getParameters()

  override def withParameters(parameters: Configuration): DataSource[T] = {
    dataSource.withParameters(parameters)
    this
  }

}
