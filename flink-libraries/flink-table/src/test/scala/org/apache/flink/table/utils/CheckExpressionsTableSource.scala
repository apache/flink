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

package org.apache.flink.table.utils

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.sources.{BatchTableSource, FilterableTableSource, StreamTableSource}
import org.apache.flink.types.Row

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

/**
  * A table source that takes in assertions and applies them when applyPredicate is called.
  * Allows for testing that expression push downs are handled properly
  * @param typeInfo The type info.
  * @param assertions A set of assertions as a function reference
  * @param pushedDown Whether this has been pushed down/
  */
class CheckExpressionsTableSource(typeInfo: RowTypeInfo,
                                  assertions: List[Expression] => Unit,
                                  pushedDown: Boolean = false)
  extends BatchTableSource[Row]
    with StreamTableSource[Row]
    with FilterableTableSource[Row] {

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.fromCollection(Collections.emptyList())
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection(Collections.emptyList())
  }

  override def applyPredicate(predicates: util.List[Expression]): CheckExpressionsTableSource = {
    assertions(predicates.asScala.toList)

    new CheckExpressionsTableSource(typeInfo, _ => {}, true)
  }

  override def isFilterPushedDown: Boolean = pushedDown

  override def getReturnType: RowTypeInfo = typeInfo
}
