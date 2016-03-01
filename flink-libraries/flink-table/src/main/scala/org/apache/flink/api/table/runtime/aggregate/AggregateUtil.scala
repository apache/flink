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
package org.apache.flink.api.table.runtime.aggregate

import java.util

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{SqlTypeFactoryImpl, SqlTypeName}
import org.apache.calcite.sql.fun._
import org.apache.flink.api.common.functions.{GroupReduceFunction, MapFunction}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.plan.PlanGenException
import org.apache.flink.api.table.plan.nodes.logical.FlinkAggregate

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object AggregateUtil {

  /**
   * Create Flink operator functions for aggregates. It includes 2 implementations of Flink 
   * operator functions:
   * [[org.apache.flink.api.common.functions.MapFunction]] and 
   * [[org.apache.flink.api.common.functions.GroupReduceFunction]](if it's partial aggregate,
   * should also implement [[org.apache.flink.api.common.functions.CombineFunction]] as well). 
   * The output of [[org.apache.flink.api.common.functions.MapFunction]] contains the 
   * intermediate aggregate values of all aggregate function, it's stored in Row by the following
   * format:
   *
   * {{{
   *                   avg(x) aggOffsetInRow = 2          count(z) aggOffsetInRow = 5
   *                             |                          |
   *                             v                          v
   *        +---------+---------+--------+--------+--------+--------+
   *        |groupKey1|groupKey2|  sum1  | count1 |  sum2  | count2 |
   *        +---------+---------+--------+--------+--------+--------+
   *                                              ^
   *                                              |
   *                               sum(y) aggOffsetInRow = 4
   * }}}
   *
   */
  def createOperatorFunctionsForAggregates(aggregate: FlinkAggregate,
      inputType: RelDataType, outputType: RelDataType,
      groupings: Array[Int]): AggregateResult = {

    val aggregateCalls: Seq[AggregateCall] = aggregate.getAggCallList
    // store the aggregate fields of each aggregate function, by the same order of aggregates.
    val aggFieldIndexes = new Array[Int](aggregateCalls.size)
    val aggregates = new Array[Aggregate[_ <: Any]](aggregateCalls.size)

    transformToAggregateFunctions(aggregateCalls, aggFieldIndexes,
      aggregates, inputType, groupings.length)

    val mapFunction = new AggregateMapFunction(aggregates, aggFieldIndexes, groupings)

    val bufferDataType: RelRecordType =
      createAggregateBufferDataType(groupings, aggregates, inputType)

    // the mapping relation between field index of intermediate aggregate Row and output Row.
    var groupingOffsetMapping = ArrayBuffer[(Int, Int)]()

    // the mapping relation between aggregate function index in list and its corresponding
    // field index in output Row.
    var aggOffsetMapping = ArrayBuffer[(Int, Int)]()


    outputType.getFieldList.zipWithIndex.foreach {
      case (fieldType: RelDataTypeField, outputIndex: Int) =>

        val aggregateIndex: Int = getMatchedAggregateIndex(aggregate, fieldType)
        if (aggregateIndex != -1) {
          aggOffsetMapping += ((outputIndex, aggregateIndex))
        } else {
          val groupKeyIndex: Int = getMatchedFieldIndex(inputType, fieldType, groupings)
          if (groupKeyIndex != -1) {
            groupingOffsetMapping += ((outputIndex, groupKeyIndex))
          } else {
            throw new PlanGenException("Could not find output field in input data type " +
                "or aggregate function.")
          }
        }
    }

    val allPartialAggregate = aggregates.map(_.supportPartial).foldLeft(true)(_ && _)

    val reduceGroupFunction =
      if (allPartialAggregate) {
        new AggregateReduceCombineFunction(aggregates, groupingOffsetMapping.toArray,
          aggOffsetMapping.toArray)
      } else {
        new AggregateReduceGroupFunction(aggregates, groupingOffsetMapping.toArray,
          aggOffsetMapping.toArray)
      }

    new AggregateResult(mapFunction, reduceGroupFunction, bufferDataType)
  }

  private def transformToAggregateFunctions(
      aggregateCalls: Seq[AggregateCall],
      aggFieldIndexes: Array[Int],
      aggregates: Array[Aggregate[_ <: Any]],
      inputType: RelDataType,
      groupKeysCount: Int): Unit = {

    // set the start offset of aggregate buffer value to group keys' length, 
    // as all the group keys would be moved to the start fields of intermediate
    // aggregate data.
    var aggOffset = groupKeysCount

    // create aggregate function instances by function type and aggregate field data type.
    aggregateCalls.zipWithIndex.foreach { case (aggregateCall, index) =>
      val argList: util.List[Integer] = aggregateCall.getArgList
      if (argList.isEmpty) {
        if (aggregateCall.getAggregation.isInstanceOf[SqlCountAggFunction]) {
          aggFieldIndexes(index) = 0
        } else {
          throw new PlanGenException("Aggregate fields should not be empty.")
        }
      } else {
        if (argList.size() > 1) {
          throw new PlanGenException("Currently, do not support aggregate on multi fields.")
        }
        aggFieldIndexes(index) = argList.get(0)
      }
      val sqlTypeName = inputType.getFieldList.get(aggFieldIndexes(index)).getType.getSqlTypeName
      aggregateCall.getAggregation match {
        case _: SqlSumAggFunction | _: SqlSumEmptyIsZeroAggFunction => {
          sqlTypeName match {
            case TINYINT =>
              aggregates(index) = new ByteSumAggregate
            case SMALLINT =>
              aggregates(index) = new ShortSumAggregate
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
          setAggregateDataOffset(index)
        }
        case _: SqlAvgAggFunction => {
          sqlTypeName match {
            case TINYINT =>
              aggregates(index) = new ByteAvgAggregate
            case SMALLINT =>
              aggregates(index) = new ShortAvgAggregate
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
          setAggregateDataOffset(index)
        }
        case sqlMinMaxFunction: SqlMinMaxAggFunction => {
          if (sqlMinMaxFunction.isMin) {
            sqlTypeName match {
              case TINYINT =>
                aggregates(index) = new ByteMinAggregate
              case SMALLINT =>
                aggregates(index) = new ShortMinAggregate
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
            sqlTypeName match {
              case TINYINT =>
                aggregates(index) = new ByteMaxAggregate
              case SMALLINT =>
                aggregates(index) = new ShortMaxAggregate
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
          setAggregateDataOffset(index)
        }
        case _: SqlCountAggFunction =>
          aggregates(index) = new CountAggregate
          setAggregateDataOffset(index)
        case unSupported: SqlAggFunction =>
          throw new PlanGenException("unsupported Function: " + unSupported.getName)
      }
    }

    // set the aggregate intermediate data start index in Row, and update current value.
    def setAggregateDataOffset(index: Int): Unit = {
      aggregates(index).setAggOffsetInRow(aggOffset)
      aggOffset += aggregates(index).intermediateDataType.length
    }
  }

  private def createAggregateBufferDataType(
      groupings: Array[Int],
      aggregates: Array[Aggregate[_]],
      inputType: RelDataType): RelRecordType = {

    // get the field data types of group keys.
    val groupingTypes: Seq[RelDataTypeField] = groupings.map(inputType.getFieldList.get(_))

    val aggPartialNameSuffix = "agg_buffer_"
    val factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)

    // get all the aggregate buffer value data type by their SqlTypeName.
    val aggTypes: Seq[RelDataTypeField] =
      aggregates.flatMap(_.intermediateDataType).zipWithIndex.map {
        case (typeName: SqlTypeName, index: Int) =>
          val fieldDataType = factory.createSqlType(typeName)
          new RelDataTypeFieldImpl(aggPartialNameSuffix + index,
            groupings.length + index, fieldDataType)
      }

    val allFieldTypes = groupingTypes ++: aggTypes
    val partialType = new RelRecordType(allFieldTypes.toList)
    partialType
  }

  private def getMatchedAggregateIndex(aggregate: FlinkAggregate,
      outputFieldType: RelDataTypeField): Int = {

    aggregate.getNamedAggCalls.zipWithIndex.foreach {
      case (namedAggCall, index) =>
        if (namedAggCall.getValue.equals(outputFieldType.getName) &&
            namedAggCall.getKey.getType.equals(outputFieldType.getType)) {
          return index
        }
    }

    -1
  }

  private def getMatchedFieldIndex(inputDatType: RelDataType,
      outputFieldType: RelDataTypeField, groupKeys: Array[Int]): Int = {
    var inputIndex = -1
    val inputFields = inputDatType.getFieldList
    // find the field index in input data type.
    for (i <- 0 until inputFields.size) {
      val inputFieldType = inputFields.get(i)
      if (outputFieldType.getName.equals(inputFieldType.getName) &&
          outputFieldType.getType.equals(inputFieldType.getType)) {
        inputIndex = i
      }
    }

    if (inputIndex != -1) {
      // as aggregated field in output data type would not have a matched field in 
      // input data, so if inputIndex is not -1, it must be a group key. Then we can 
      // find the field index in buffer data by the group keys index mapping between 
      // input data and buffer data.
      for (i <- 0 until groupKeys.length) {
        if (inputIndex == groupKeys(i)) {
          return i
        }
      }
    }

    -1
  }
}

case class AggregateResult(
    val mapFunc: MapFunction[Row, Row],
    val reduceGroupFunc: GroupReduceFunction[Row, Row],
    val intermediateDataType: RelDataType) {
}
