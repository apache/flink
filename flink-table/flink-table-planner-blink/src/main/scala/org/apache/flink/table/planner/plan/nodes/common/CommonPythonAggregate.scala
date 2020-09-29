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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{DataTypes, TableException}
import org.apache.flink.table.api.dataview.ListView
import org.apache.flink.table.dataview.ListViewTypeInfo
import org.apache.flink.table.functions.python.{PythonAggregateFunction, PythonFunction, PythonFunctionInfo}
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.planner.typeutils.DataViewUtils.{DataViewSpec, ListViewSpec}
import org.apache.flink.table.types.utils.TypeConversions

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
      aggIndex: Int,
      pythonAggregateInfo: AggregateInfo): (PythonFunctionInfo, Array[DataViewSpec]) = {
    pythonAggregateInfo.function match {
      case function: PythonFunction =>
        (
          new PythonFunctionInfo(
            function,
            pythonAggregateInfo.argIndexes.map(_.asInstanceOf[AnyRef])),
          extractDataViewSpecs(
            aggIndex,
            function.asInstanceOf[PythonAggregateFunction].getAccumulatorType)
        )
      case _: Count1AggFunction =>
        // The count star will be treated specially in Python UDF worker
        (PythonFunctionInfo.DUMMY_PLACEHOLDER, Array())
      case _ =>
        throw new TableException(
          "Unsupported python aggregate function: " + pythonAggregateInfo.function)
    }
  }

  protected def extractDataViewSpecs(
      index: Int,
      accType: TypeInformation[_]): Array[DataViewSpec] = {
    if (!accType.isInstanceOf[CompositeType[_]]) {
      return Array()
    }

    def includesDataView(ct: CompositeType[_]): Boolean = {
      (0 until ct.getArity).exists(i =>
        ct.getTypeAt(i) match {
          case nestedCT: CompositeType[_] => includesDataView(nestedCT)
          case t: TypeInformation[_] if t.getTypeClass == classOf[ListView[_]] => true
          case _ => false
        }
      )
    }

    if (includesDataView(accType.asInstanceOf[CompositeType[_]])) {
      accType match {
        case rowType: RowTypeInfo =>
            (0 until rowType.getArity).flatMap(i => {
              rowType.getFieldTypes()(i) match {
                case ct: CompositeType[_] if includesDataView(ct) =>
                  throw new TableException(
                    "For Python AggregateFunction DataView only supported at first " +
                      "level of accumulators of Row type.")
                case listView: ListViewTypeInfo[_] =>
                  Some(new ListViewSpec(
                    "agg" + index + "$" + rowType.getFieldNames()(i),
                    i,
                    DataTypes.ARRAY(
                      TypeConversions.fromLegacyInfoToDataType(listView.getElementType))))
                case _ =>
                  None
              }
            }).toArray
        case _ =>
          throw new TableException("For Python AggregateFunction you can only use DataView in " +
            "Row type.")
      }
    } else {
      Array()
    }
  }
}
