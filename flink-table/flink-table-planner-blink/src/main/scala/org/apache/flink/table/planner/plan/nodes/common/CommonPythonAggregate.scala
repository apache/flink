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
import org.apache.flink.table.api.dataview.{DataView, ListView, MapView}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.python._
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction._
import org.apache.flink.table.planner.functions.aggfunctions._
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfoList
import org.apache.flink.table.planner.typeutils.DataViewUtils.{DataViewSpec, ListViewSpec, MapViewSpec}
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
        new PythonAggregateFunctionInfo(
          pythonFunction, inputs.toArray, aggregateCall.filterArg, aggregateCall.isDistinct)
    }
    val udafInputOffsets = inputNodes.toArray.map(_._1.toInt)
    (udafInputOffsets, pythonFunctionInfos.toArray)
  }

  /**
    * For streaming execution we extract the PythonFunctionInfo from both AggregateInfo and
    * AggregateCall.
    */
  protected def extractPythonAggregateFunctionInfos(
      pythonAggregateInfoList: AggregateInfoList, aggCalls: Seq[AggregateCall]):
  (Array[PythonAggregateFunctionInfo], Array[Array[DataViewSpec]]) = {
    val pythonAggregateFunctionInfoList = new mutable.ArrayBuffer[PythonAggregateFunctionInfo]
    val dataViewSpecList = new mutable.ArrayBuffer[Array[DataViewSpec]]
    for (i <- pythonAggregateInfoList.aggInfos.indices) {
      pythonAggregateInfoList.aggInfos(i).function match {
        case function: PythonFunction =>
          pythonAggregateFunctionInfoList.add(new PythonAggregateFunctionInfo(
            function,
            pythonAggregateInfoList.aggInfos(i).argIndexes.map(_.asInstanceOf[AnyRef]),
            aggCalls(i).filterArg,
            aggCalls(i).isDistinct))
          val typeInference = function match {
            case aggregateFunction: PythonAggregateFunction =>
              aggregateFunction.getTypeInference(null)
            case tableAggregateFunction: PythonTableAggregateFunction =>
              tableAggregateFunction.getTypeInference(null)
          }
          dataViewSpecList.add(
            extractDataViewSpecs(
              i,
              typeInference.getAccumulatorTypeStrategy.get().inferType(null).get()))
        case function: UserDefinedFunction =>
          var filterArg = -1
          var distinct = false
          if (i < aggCalls.size) {
            filterArg = aggCalls(i).filterArg
            distinct = aggCalls(i).isDistinct
          }
          pythonAggregateFunctionInfoList.add(new PythonAggregateFunctionInfo(
            getBuiltInPythonAggregateFunction(function),
            pythonAggregateInfoList.aggInfos(i).argIndexes.map(_.asInstanceOf[AnyRef]),
            filterArg,
            distinct))
          // The data views of the built in Python Aggregate Function are different from Java side,
          // we will create the spec at Python side.
          dataViewSpecList.add(Array())
        case _ =>
          throw new TableException(
            "Unsupported python aggregate function: " +
              pythonAggregateInfoList.aggInfos(i).function)
      }
    }
    (pythonAggregateFunctionInfoList.toArray, dataViewSpecList.toArray)
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
                case mapViewType: StructuredType if classOf[MapView[_, _]].isAssignableFrom(
                  mapViewType.getImplementationClass.get()) =>
                  Some(new MapViewSpec(
                    "agg" + index + "$" + rowType.getFieldNames()(i),
                    i,
                    compositeAccType.getChildren.get(i).asInstanceOf[FieldsDataType]
                      .getChildren.get(0),
                    false).asInstanceOf[DataViewSpec])
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

  private def getBuiltInPythonAggregateFunction(
      javaBuiltInAggregateFunction: UserDefinedFunction): BuiltInPythonAggregateFunction = {
    javaBuiltInAggregateFunction match {
      case _: AvgAggFunction =>
        BuiltInPythonAggregateFunction.AVG
      case _: Count1AggFunction =>
        BuiltInPythonAggregateFunction.COUNT1
      case _: CountAggFunction =>
        BuiltInPythonAggregateFunction.COUNT
      case _: FirstValueAggFunction[_] =>
        BuiltInPythonAggregateFunction.FIRST_VALUE
      case _: FirstValueWithRetractAggFunction[_] =>
        BuiltInPythonAggregateFunction.FIRST_VALUE_RETRACT
      case _: LastValueAggFunction[_] =>
        BuiltInPythonAggregateFunction.LAST_VALUE
      case _: LastValueWithRetractAggFunction[_] =>
        BuiltInPythonAggregateFunction.LAST_VALUE_RETRACT
      case _: ListAggFunction =>
        BuiltInPythonAggregateFunction.LIST_AGG
      case _: ListAggWithRetractAggFunction =>
        BuiltInPythonAggregateFunction.LIST_AGG_RETRACT
      case _: ListAggWsWithRetractAggFunction =>
        BuiltInPythonAggregateFunction.LIST_AGG_WS_RETRACT
      case _: MaxAggFunction =>
        BuiltInPythonAggregateFunction.MAX
      case _: MaxWithRetractAggFunction[_] =>
        BuiltInPythonAggregateFunction.MAX_RETRACT
      case _: MinAggFunction =>
        BuiltInPythonAggregateFunction.MIN
      case _: MinWithRetractAggFunction[_] =>
        BuiltInPythonAggregateFunction.MIN_RETRACT
      case _: SumAggFunction =>
        BuiltInPythonAggregateFunction.SUM
      case _: IntSum0AggFunction | _: ByteSum0AggFunction | _: ShortSum0AggFunction |
           _: LongSum0AggFunction =>
        BuiltInPythonAggregateFunction.INT_SUM0
      case _: FloatSum0AggFunction | _: DoubleSum0AggFunction =>
        BuiltInPythonAggregateFunction.FLOAT_SUM0
      case _: DecimalSum0AggFunction =>
        BuiltInPythonAggregateFunction.DECIMAL_SUM0
      case _: SumWithRetractAggFunction =>
        BuiltInPythonAggregateFunction.SUM_RETRACT
      case _ =>
        throw new TableException("Aggregate function " + javaBuiltInAggregateFunction +
          " is still not supported to be mixed with Python UDAF.")
    }
  }
}
