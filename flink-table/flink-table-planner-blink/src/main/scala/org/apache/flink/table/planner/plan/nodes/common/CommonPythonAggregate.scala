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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.calcite.rel.core.AggregateCall
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo}
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfo

import scala.collection.JavaConversions._
import scala.collection.mutable

trait CommonPythonAggregate extends CommonPythonBase {

  /**
    * For batch execution we extract the PythonFunctionInfo from AggregateCall.
    */
  protected def extractPythonAggregateFunctionInfosFromAggregateCall(
      aggCalls: Seq[AggregateCall]): (Array[Int], Array[PythonFunctionInfo]) = {
    val inputNodes = new mutable.LinkedHashMap[Integer, Integer]()
    val pythonFunctionInfos: Seq[PythonFunctionInfo] = aggCalls.map {
      aggregateCall: AggregateCall =>
        val inputs = new mutable.ArrayBuffer[AnyRef]()
        val argList = aggregateCall.getArgList
        for (arg <- argList) {
          inputNodes.get(arg) match {
            case Some(existing) => inputs.append(existing)
            case None => val inputOffset = Integer.valueOf(inputNodes.size)
              inputs.append(inputOffset)
              inputNodes.put(arg, inputOffset)
          }
        }
        val pythonFunction = aggregateCall.getAggregation match {
          case function: AggSqlFunction =>
            function.aggregateFunction.asInstanceOf[PythonFunction]
          case function: BridgingSqlAggFunction =>
            function.getDefinition.asInstanceOf[PythonFunction]
        }
        new PythonFunctionInfo(pythonFunction, inputs.toArray)
    }
    val udafInputOffsets = inputNodes.toArray.map(_._1.toInt)
    (udafInputOffsets, pythonFunctionInfos.toArray)
  }

  /**
    * For streaming execution we extract the PythonFunctionInfo from AggregateInfo.
    */
  protected def extractPythonAggregateFunctionInfosFromAggregateInfo(
      pythonAggregateInfo: AggregateInfo): PythonFunctionInfo = {
    pythonAggregateInfo.function match {
      case function: PythonFunction =>
        new PythonFunctionInfo(
          function,
          pythonAggregateInfo.argIndexes.map(_.asInstanceOf[AnyRef]))
      case _: Count1AggFunction =>
        // The count star will be treated specially in Python UDF worker
        PythonFunctionInfo.DUMMY_PLACEHOLDER
      case _ =>
        throw new TableException(
          "Unsupported python aggregate function: " + pythonAggregateInfo.function)
    }
  }
}
