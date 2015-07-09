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

package org.apache.flink.api.table.input

import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * StaticTableSources are an easy way to provide the Table API with additional input formats.
 * For more advanced TableSources that also adapt to field accesses and
 * predicates see [[AdaptiveTableSource]].
 */
trait StaticTableSource extends TableSource {

  /**
   * Define name and order of output fields.
   *
   * @return output field names
   */
  def getOutputFieldNames(): Seq[String]

  /**
   * Returns a [[DataSet]] of a [[org.apache.flink.api.common.typeutils.CompositeType]].
   *
   * @param env execution environment
   * @return data set
   */
  def createStaticDataSet(env: ExecutionEnvironment) : DataSet[_]

  /**
   * Returns a [[DataStream]] of a [[org.apache.flink.api.common.typeutils.CompositeType]].
   *
   * @param env execution environment
   * @return data stream
   */
  def createStaticDataStream(env: StreamExecutionEnvironment) : DataStream[_]

}
