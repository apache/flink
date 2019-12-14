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

package org.apache.flink.table.calcite

import org.apache.calcite.plan.{Context, Contexts, RelOptCluster, RelOptSchema}
import org.apache.calcite.rel.core.RelFactories._
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.flink.table.expressions.{ExpressionBridge, PlannerExpression}
/**
  * The utility class is to create special [[RelBuilder]] instance
  * this factory ignores merge projection operations.
  */
class FlinkRelBuilderFactory(
    expressionBridge: ExpressionBridge[PlannerExpression])
  extends RelBuilderFactory {

  val context: Context = Contexts.of(Array[AnyRef](
    DEFAULT_PROJECT_FACTORY,
    DEFAULT_FILTER_FACTORY,
    DEFAULT_JOIN_FACTORY,
    DEFAULT_SORT_FACTORY,
    DEFAULT_EXCHANGE_FACTORY,
    DEFAULT_SORT_EXCHANGE_FACTORY,
    DEFAULT_AGGREGATE_FACTORY,
    DEFAULT_MATCH_FACTORY,
    DEFAULT_SET_OP_FACTORY,
    DEFAULT_VALUES_FACTORY,
    DEFAULT_TABLE_SCAN_FACTORY))

  override def create(relOptCluster: RelOptCluster, relOptSchema: RelOptSchema): RelBuilder = {
    new FlinkRelBuilder(context, relOptCluster, relOptSchema, expressionBridge)
  }
}
