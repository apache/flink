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
package org.apache.flink.api.table.plan.functions.aggregate

import java.util

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun._
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.table.plan.PlanGenException
import org.apache.flink.api.table.plan.functions.AggregateFunction

object AggregateFactory {

  def createAggregateInstance(aggregateCalls: Seq[AggregateCall]):
    RichGroupReduceFunction[Any, Any] = {

    val fieldIndexes = new Array[Int](aggregateCalls.size)
    val aggregates = new Array[Aggregate[_ <: Any]](aggregateCalls.size)
    aggregateCalls.zipWithIndex.map { case (aggregateCall, index) =>
      val sqlType = aggregateCall.getType
      val argList: util.List[Integer] = aggregateCall.getArgList
      // currently assume only aggregate on singleton field.
      if (argList.isEmpty) {
        if (aggregateCall.getAggregation.isInstanceOf[SqlCountAggFunction]) {
          fieldIndexes(index) = 0
        } else {
          throw new PlanGenException("Aggregate fields should not be empty.")
        }
      } else {
        fieldIndexes(index) = argList.get(0);
      }
      aggregateCall.getAggregation match {
        case _: SqlSumAggFunction | _: SqlSumEmptyIsZeroAggFunction => {
          sqlType.getSqlTypeName match {
            case TINYINT =>
              aggregates(index) = new TinyIntSumAggregate
            case SMALLINT =>
              aggregates(index) = new SmallIntSumAggregate
            case INTEGER =>
              aggregates(index) = new IntSumAggregate
            case BIGINT =>
              aggregates(index) = new LongSumAggregate
            case FLOAT =>
              aggregates(index) = new FloatSumAggregate
            case DOUBLE =>
              aggregates(index) = new DoubleSumAggregate
            case sqlType: SqlTypeName =>
              throw new PlanGenException("Sum aggregate does no support type:" + sqlType)
          }
        }
        case _: SqlAvgAggFunction => {
          sqlType.getSqlTypeName match {
            case TINYINT =>
              aggregates(index) = new TinyIntAvgAggregate
            case SMALLINT =>
              aggregates(index) = new SmallIntAvgAggregate
            case INTEGER =>
              aggregates(index) = new IntAvgAggregate
            case BIGINT =>
              aggregates(index) = new LongAvgAggregate
            case FLOAT =>
              aggregates(index) = new FloatAvgAggregate
            case DOUBLE =>
              aggregates(index) = new DoubleAvgAggregate
            case sqlType: SqlTypeName =>
              throw new PlanGenException("Avg aggregate does no support type:" + sqlType)
          }
        }
        case sqlMinMaxFunction: SqlMinMaxAggFunction => {
          if (sqlMinMaxFunction.isMin) {
            sqlType.getSqlTypeName match {
              case TINYINT =>
                aggregates(index) = new TinyIntMinAggregate
              case SMALLINT =>
                aggregates(index) = new SmallIntMinAggregate
              case INTEGER =>
                aggregates(index) = new IntMinAggregate
              case BIGINT =>
                aggregates(index) = new LongMinAggregate
              case FLOAT =>
                aggregates(index) = new FloatMinAggregate
              case DOUBLE =>
                aggregates(index) = new DoubleMinAggregate
              case sqlType: SqlTypeName =>
                throw new PlanGenException("Min aggregate does no support type:" + sqlType)
            }
          } else {
            sqlType.getSqlTypeName match {
              case TINYINT =>
                aggregates(index) = new TinyIntMaxAggregate
              case SMALLINT =>
                aggregates(index) = new SmallIntMaxAggregate
              case INTEGER =>
                aggregates(index) = new IntMaxAggregate
              case BIGINT =>
                aggregates(index) = new LongMaxAggregate
              case FLOAT =>
                aggregates(index) = new FloatMaxAggregate
              case DOUBLE =>
                aggregates(index) = new DoubleMaxAggregate
              case sqlType: SqlTypeName =>
                throw new PlanGenException("Max aggregate does no support type:" + sqlType)
            }
          }
        }
        case _: SqlCountAggFunction =>
          aggregates(index) = new CountAggregate
        case unSupported: SqlAggFunction =>
          throw new PlanGenException("unsupported Function: " + unSupported.getName)
      }
    }

    new AggregateFunction(aggregates, fieldIndexes)
  }

}
