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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * AdaptiveTableSources can adapt their output to the requirements of the plan.
 * Although the output schema stays the same, the TableSource can react on
 * field resolution and/or predicates internally and can return adapted DataSet/DataStream
 * versions in the "translate" step.
 */
trait AdaptiveTableSource extends TableSource {

  /**
   * Returns the schema of the resulting rows. The schema remains constant even if the
   * TableSource adapts to information passed in notify-methods.
   *
   * @return schema of the result rows
   */
  def getOutputFields(): Seq[(String, TypeInformation[_])]

  /**
   * Notifies the TableSource about a resolved field.
   *
   * @param field resolved field name
   * @param filtering `true` if field has been resolved as part of a filter
   *                  operation. At least one `false` indicates that a wildcard
   *                  selection does not take place.
   */
  def notifyResolvedField(field: String, filtering: Boolean)

  /**
   * Notifies the TableSource about predicates that apply to its fields. The
   * predicates are already cleaned such that they only contain comparisons
   * with literals `a===5 or b==='10'` or `a.notNull and c.isNull`.
   *
   * @param expression expression tree only consisting of predicates relevant for this
   *                   TableSource with literal comparisons
   */
  def notifyPredicates(expression: Expression)

  /**
   * Returns the [[DataSet]] of [[Row]]s that sticks to the schema defined in `getOutputFields()`.
   * The TableSource may has modified unresolved fields (e.g. has fields set to `null`) and
   * added pre-filters.
   *
   * @param env execution environment
   * @return adapted data set
   */
  def createAdaptiveDataSet(env: ExecutionEnvironment) : DataSet[Row]

  /**
   * Returns the [[DataStream]] of [[Row]]s that sticks to schema defined in `getOutputFields()`.
   * The TableSource may has modified unresolved fields (e.g. has fields set to `null`) and
   * added pre-filters.
   *
   * @param env stream execution environment
   * @return adapted data stream
   */
  def createAdaptiveDataStream(env: StreamExecutionEnvironment) : DataStream[Row]

}
