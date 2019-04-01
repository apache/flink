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
package org.apache.flink.table.plan.util

import org.apache.flink.table.`type`.InternalTypes._
import org.apache.flink.table.`type`.{DecimalType, InternalType}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.typeutils.DecimalTypeInfo

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.{SqlAggFunction, SqlKind}

import scala.collection.JavaConversions._

/**
  * The class of agg function factory which is used to create AggregateFunction or
  * DeclarativeAggregateFunction from Calcite AggregateCall
  *
  * @param inputType the input rel data type
  * @param orderKeyIdx the indexes of order key (null when is not over agg)
  * @param needRetraction true if need retraction
  */
class AggFunctionFactory(
    inputType: RelDataType,
    orderKeyIdx: Array[Int],
    needRetraction: Array[Boolean]) {

  /**
    * The entry point to create an aggregate function from the given AggregateCall
    */
  def createAggFunction(call: AggregateCall, index: Int): UserDefinedFunction = {

    val argTypes: Array[InternalType] = call.getArgList
      .map(inputType.getFieldList.get(_).getType) // RelDataType
      .map(FlinkTypeFactory.toInternalType) // InternalType
      .toArray

    call.getAggregation match {
      case a: SqlAvgAggFunction if a.kind == SqlKind.AVG => createAvgAggFunction(argTypes)

      case _: SqlSumAggFunction => createSumAggFunction(argTypes, index)

      case _: SqlSumEmptyIsZeroAggFunction => createSum0AggFunction(argTypes)

      // TODO supports SqlIncrSumAggFunction

      case a: SqlMinMaxAggFunction if a.getKind == SqlKind.MIN =>
        createMinAggFunction(argTypes, index)

      case a: SqlMinMaxAggFunction if a.getKind == SqlKind.MAX =>
        createMaxAggFunction(argTypes, index)

      case _: SqlCountAggFunction if call.getArgList.size() > 1 =>
        throw new TableException("We now only support the count of one field.")

      // TODO supports ApproximateCountDistinctAggFunction and CountDistinctAggFunction

      case _: SqlCountAggFunction if call.getArgList.isEmpty => createCount1AggFunction(argTypes)

      case _: SqlCountAggFunction => createCountAggFunction(argTypes)

      // TODO supports SqlRankFunction (ROW_NUMBER, RANK and DENSE_RANK)
      // TODO supports SqlLeadLagAggFunction
      // TODO supports SqlMax2ndAggFunction
      // TODO supports SqlSingleValueAggFunction
      // TODO supports SqlFirstLastValueAggFunction (FIRST_VALUE and LAST_VALUE)
      // TODO supports SqlConcatAggFunction
      // TODO supports SqlCardinalityCountAggFunction
      // TODO supports COLLECT SqlAggFunction

      case udagg: AggSqlFunction => udagg.getFunction

      case unSupported: SqlAggFunction =>
        throw new TableException(s"Unsupported Function: '${unSupported.getName}'")
    }
  }

  private def createAvgAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    argTypes(0) match {
      case BYTE | SHORT | INT | LONG =>
        new org.apache.flink.table.functions.AvgAggFunction.IntegralAvgAggFunction
      case FLOAT | DOUBLE =>
        new org.apache.flink.table.functions.AvgAggFunction.DoubleAvgAggFunction
      case d: DecimalType =>
        val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
        new org.apache.flink.table.functions.AvgAggFunction.DecimalAvgAggFunction(decimalTypeInfo)
      case t: InternalType =>
        throw new TableException(s"Avg aggregate function does not support type: ''$t''.\n" +
          s"Please re-check the data type.")
    }
  }

  private def createSumAggFunction(argTypes: Array[InternalType], index: Int)
  : UserDefinedFunction = {
    if (needRetraction(index)) {
      // TODO implements this
      throw new TableException("Unsupported now")
    } else {
      argTypes(0) match {
        case BYTE =>
          new org.apache.flink.table.functions.SumAggFunction.ByteSumAggFunction
        case SHORT =>
          new org.apache.flink.table.functions.SumAggFunction.ShortSumAggFunction
        case INT =>
          new org.apache.flink.table.functions.SumAggFunction.IntSumAggFunction
        case LONG =>
          new org.apache.flink.table.functions.SumAggFunction.LongSumAggFunction
        case FLOAT =>
          new org.apache.flink.table.functions.SumAggFunction.FloatSumAggFunction
        case DOUBLE =>
          new org.apache.flink.table.functions.SumAggFunction.DoubleSumAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new org.apache.flink.table.functions.SumAggFunction.DecimalSumAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"Sum aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createSum0AggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    argTypes(0) match {
      case BYTE =>
        new org.apache.flink.table.functions.Sum0AggFunction.ByteSum0AggFunction
      case SHORT =>
        new org.apache.flink.table.functions.Sum0AggFunction.ShortSum0AggFunction
      case INT =>
        new org.apache.flink.table.functions.Sum0AggFunction.IntSum0AggFunction
      case LONG =>
        new org.apache.flink.table.functions.Sum0AggFunction.LongSum0AggFunction
      case FLOAT =>
        new org.apache.flink.table.functions.Sum0AggFunction.FloatSum0AggFunction
      case DOUBLE =>
        new org.apache.flink.table.functions.Sum0AggFunction.DoubleSum0AggFunction
      case d: DecimalType =>
        val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
        new org.apache.flink.table.functions.Sum0AggFunction.DecimalSum0AggFunction(decimalTypeInfo)
      case t: InternalType =>
        throw new TableException(s"Sum0 aggregate function does not support type: ''$t''.\n" +
          s"Please re-check the data type.")
    }
  }

  private def createMinAggFunction(
      argTypes: Array[InternalType], index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      // TODO implements this
      throw new TableException("Unsupported now")
    } else {
      argTypes(0) match {
        case BYTE =>
          new org.apache.flink.table.functions.MinAggFunction.ByteMinAggFunction
        case SHORT =>
          new org.apache.flink.table.functions.MinAggFunction.ShortMinAggFunction
        case INT =>
          new org.apache.flink.table.functions.MinAggFunction.IntMinAggFunction
        case LONG =>
          new org.apache.flink.table.functions.MinAggFunction.LongMinAggFunction
        case FLOAT =>
          new org.apache.flink.table.functions.MinAggFunction.FloatMinAggFunction
        case DOUBLE =>
          new org.apache.flink.table.functions.MinAggFunction.DoubleMinAggFunction
        case BOOLEAN =>
          new org.apache.flink.table.functions.MinAggFunction.BooleanMinAggFunction
        case STRING =>
          new org.apache.flink.table.functions.MinAggFunction.StringMinAggFunction
        case DATE =>
          new org.apache.flink.table.functions.MinAggFunction.DateMinAggFunction
        case TIME =>
          new org.apache.flink.table.functions.MinAggFunction.TimeMinAggFunction
        case TIMESTAMP |  PROCTIME_INDICATOR | ROWTIME_INDICATOR =>
          new org.apache.flink.table.functions.MinAggFunction.TimestampMinAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new org.apache.flink.table.functions.MinAggFunction.DecimalMinAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"Min aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createMaxAggFunction(
      argTypes: Array[InternalType], index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      // TODO implements this
      throw new TableException("Unsupported now")
    } else {
      argTypes(0) match {
        case BYTE =>
          new org.apache.flink.table.functions.MaxAggFunction.ByteMaxAggFunction
        case SHORT =>
          new org.apache.flink.table.functions.MaxAggFunction.ShortMaxAggFunction
        case INT =>
          new org.apache.flink.table.functions.MaxAggFunction.IntMaxAggFunction
        case LONG =>
          new org.apache.flink.table.functions.MaxAggFunction.LongMaxAggFunction
        case FLOAT =>
          new org.apache.flink.table.functions.MaxAggFunction.FloatMaxAggFunction
        case DOUBLE =>
          new org.apache.flink.table.functions.MaxAggFunction.DoubleMaxAggFunction
        case BOOLEAN =>
          new org.apache.flink.table.functions.MaxAggFunction.BooleanMaxAggFunction
        case STRING =>
          new org.apache.flink.table.functions.MaxAggFunction.StringMaxAggFunction
        case DATE =>
          new org.apache.flink.table.functions.MaxAggFunction.DateMaxAggFunction
        case TIME =>
          new org.apache.flink.table.functions.MaxAggFunction.TimeMaxAggFunction
        case TIMESTAMP | PROCTIME_INDICATOR | ROWTIME_INDICATOR =>
          new org.apache.flink.table.functions.MaxAggFunction.TimestampMaxAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new org.apache.flink.table.functions.MaxAggFunction.DecimalMaxAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"Max aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createCount1AggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    new org.apache.flink.table.functions.Count1AggFunction
  }

  private def createCountAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    new org.apache.flink.table.functions.CountAggFunction
  }
}
