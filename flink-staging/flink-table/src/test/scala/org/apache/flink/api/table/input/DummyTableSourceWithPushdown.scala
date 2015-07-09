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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

class DummyTableSourceWithPushdown extends AdaptiveTableSource {

  val resolvedFields = Set[(String, Boolean)]()
  val predicates = ListBuffer[Expression]()

  override def getOutputFields(): Seq[(String, TypeInformation[_])] = {
    List(("a", BasicTypeInfo.INT_TYPE_INFO),
      ("b", BasicTypeInfo.INT_TYPE_INFO),
      ("c", BasicTypeInfo.STRING_TYPE_INFO))
  }

  override def supportsResolvedFieldPushdown(): Boolean = true

  override def supportsPredicatePushdown(): Boolean = true

  override def notifyResolvedField(field: String, filtering: Boolean): Unit = {
    resolvedFields += ((field, filtering))
  }

  override def notifyPredicates(expression: Expression): Unit = {
    predicates += expression
  }

  override def createAdaptiveDataStream(env: StreamExecutionEnvironment): DataStream[Row] = null

  override def createAdaptiveDataSet(env: ExecutionEnvironment): DataSet[Row] = null

}
