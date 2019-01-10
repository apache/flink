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
package org.apache.flink.table.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.expressions.{AggFunctionCall, DistinctAgg, Expression}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getAccumulatorTypeOfAggregateFunction, getResultTypeOfAggregateFunction}

/**
  * Defines an implicit conversion method (distinct) that converts [[AggregateFunction]]s into
  * [[DistinctAgg]] Expressions.
  */
private[flink] case class DistinctAggregateFunction[T: TypeInformation, ACC: TypeInformation]
    (aggFunction: AggregateFunction[T, ACC]) {

  def distinct(params: Expression*): Expression = {
    val resultTypeInfo: TypeInformation[_] = getResultTypeOfAggregateFunction(
      aggFunction,
      implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[_] = getAccumulatorTypeOfAggregateFunction(
      aggFunction,
      implicitly[TypeInformation[ACC]])

    DistinctAgg(
      AggFunctionCall(aggFunction, resultTypeInfo, accTypeInfo, params))
  }
}
