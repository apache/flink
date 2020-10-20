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
import org.apache.flink.table.api.dataview.{DataView, ListView}
import org.apache.flink.table.functions.python.{PythonAggregateFunction, PythonFunction, PythonFunctionInfo}
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.planner.typeutils.DataViewUtils.{DataViewSpec, ListViewSpec}
import org.apache.flink.table.types.logical.{RowType, StructuredType}
import org.apache.flink.table.types.{DataType, FieldsDataType}

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
            function.asInstanceOf[PythonAggregateFunction].getTypeInference(null)
              .getAccumulatorTypeStrategy.get()
              .inferType(null).get())
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
      accType: DataType): Array[DataViewSpec] = {
    if (!accType.isInstanceOf[FieldsDataType]) {
      return Array()
    }

    val compositeAccType = accType.asInstanceOf[FieldsDataType]

    def includesDataView(fdt: FieldsDataType): Boolean = {
      (0 until fdt.getChildren.size()).exists(i =>
        fdt.getChildren.get(i).getLogicalType match {
          case _: RowType =>
            includesDataView(fdt.getChildren.get(i).asInstanceOf[FieldsDataType])
          case structuredType: StructuredType =>
            classOf[DataView].isAssignableFrom(structuredType.getImplementationClass.get())
          case _ => false
        }
      )
    }

    if (includesDataView(compositeAccType)) {
      compositeAccType.getLogicalType match {
        case rowType: RowType =>
            (0 until compositeAccType.getChildren.size()).flatMap(i => {
              compositeAccType.getChildren.get(i).getLogicalType match {
                case _: RowType if includesDataView(
                  compositeAccType.getChildren.get(i).asInstanceOf[FieldsDataType]) =>
                  throw new TableException(
                    "For Python AggregateFunction, DataView cannot be used in the nested columns " +
                      "of the accumulator. ")
                case listViewType: StructuredType if classOf[ListView[_]].isAssignableFrom(
                  listViewType.getImplementationClass.get()) =>
                  Some(new ListViewSpec(
                    "agg" + index + "$" + rowType.getFieldNames()(i),
                    i,
                    compositeAccType.getChildren.get(i).asInstanceOf[FieldsDataType]
                      .getChildren.get(0)))
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
