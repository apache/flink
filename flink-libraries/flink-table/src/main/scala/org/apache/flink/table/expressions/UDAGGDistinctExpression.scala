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
package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getAccumulatorTypeOfAggregateFunction, getResultTypeOfAggregateFunction}
import org.apache.flink.table.functions.{AggregateFunction, DistinctAggregateFunction}

/**
  * A class which creates a distinct aggregation wrap to a call to an aggregateFunction.
  */
case class UDAGGDistinctExpression[T: TypeInformation, ACC: TypeInformation](
    distinctAggregateFunction: DistinctAggregateFunction[T, ACC]) {

  /**
    * Creates a DistinctAgg that wraps the actual call to an [[AggregateFunction]].
    *
    * @param params actual parameters of function
    * @return a [[DistinctAgg]]
    */
  def apply(params: Expression*): DistinctAgg = {
    val resultTypeInfo: TypeInformation[_] = getResultTypeOfAggregateFunction(
      distinctAggregateFunction.aggFunction,
      implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[_] = getAccumulatorTypeOfAggregateFunction(
      distinctAggregateFunction.aggFunction,
      implicitly[TypeInformation[ACC]])

    DistinctAgg(
      AggFunctionCall(distinctAggregateFunction.aggFunction, resultTypeInfo, accTypeInfo, params))
  }
}
