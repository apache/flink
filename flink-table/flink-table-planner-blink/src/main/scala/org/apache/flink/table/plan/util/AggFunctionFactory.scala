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
import org.apache.flink.table.functions.aggfunctions.MaxWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.MinWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.SumWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.{AvgAggFunction, Count1AggFunction, CountAggFunction, MaxAggFunction, MinAggFunction, Sum0AggFunction, SumAggFunction}
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.functions.{LeadLagAggFunction, UserDefinedFunction}
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

      // TODO supports SqlMax2ndAggFunction
      // TODO supports SqlSingleValueAggFunction
      // TODO supports SqlFirstLastValueAggFunction (FIRST_VALUE and LAST_VALUE)
      // TODO supports SqlConcatAggFunction
      // TODO supports SqlCardinalityCountAggFunction
      // TODO supports COLLECT SqlAggFunction

      case _: SqlLeadLagAggFunction =>
        createLeadLagAggFunction(argTypes, index)

      case udagg: AggSqlFunction => udagg.getFunction

      case unSupported: SqlAggFunction =>
        throw new TableException(s"Unsupported Function: '${unSupported.getName}'")
    }
  }

  private def createAvgAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    argTypes(0) match {
      case BYTE | SHORT | INT | LONG =>
        new AvgAggFunction.IntegralAvgAggFunction
      case FLOAT | DOUBLE =>
        new AvgAggFunction.DoubleAvgAggFunction
      case d: DecimalType =>
        val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
        new AvgAggFunction.DecimalAvgAggFunction(decimalTypeInfo)
      case t: InternalType =>
        throw new TableException(s"Avg aggregate function does not support type: ''$t''.\n" +
          s"Please re-check the data type.")
    }
  }

  private def createSumAggFunction(
      argTypes: Array[InternalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0) match {
        case BYTE =>
          new ByteSumWithRetractAggFunction
        case SHORT =>
          new ShortSumWithRetractAggFunction
        case INT =>
          new IntSumWithRetractAggFunction
        case LONG =>
          new LongSumWithRetractAggFunction
        case FLOAT =>
          new FloatSumWithRetractAggFunction
        case DOUBLE =>
          new DoubleSumWithRetractAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalSumWithRetractAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"Sum with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0) match {
        case BYTE =>
          new SumAggFunction.ByteSumAggFunction
        case SHORT =>
          new SumAggFunction.ShortSumAggFunction
        case INT =>
          new SumAggFunction.IntSumAggFunction
        case LONG =>
          new SumAggFunction.LongSumAggFunction
        case FLOAT =>
          new SumAggFunction.FloatSumAggFunction
        case DOUBLE =>
          new SumAggFunction.DoubleSumAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new SumAggFunction.DecimalSumAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"Sum aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createSum0AggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    argTypes(0) match {
      case BYTE =>
        new Sum0AggFunction.ByteSum0AggFunction
      case SHORT =>
        new Sum0AggFunction.ShortSum0AggFunction
      case INT =>
        new Sum0AggFunction.IntSum0AggFunction
      case LONG =>
        new Sum0AggFunction.LongSum0AggFunction
      case FLOAT =>
        new Sum0AggFunction.FloatSum0AggFunction
      case DOUBLE =>
        new Sum0AggFunction.DoubleSum0AggFunction
      case d: DecimalType =>
        val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
        new Sum0AggFunction.DecimalSum0AggFunction(decimalTypeInfo)
      case t: InternalType =>
        throw new TableException(s"Sum0 aggregate function does not support type: ''$t''.\n" +
          s"Please re-check the data type.")
    }
  }

  private def createMinAggFunction(
      argTypes: Array[InternalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0) match {
        case BYTE =>
          new ByteMinWithRetractAggFunction
        case SHORT =>
          new ShortMinWithRetractAggFunction
        case INT =>
          new IntMinWithRetractAggFunction
        case LONG =>
          new LongMinWithRetractAggFunction
        case FLOAT =>
          new FloatMinWithRetractAggFunction
        case DOUBLE =>
          new DoubleMinWithRetractAggFunction
        case BOOLEAN =>
          new BooleanMinWithRetractAggFunction
        case STRING =>
          new StringMinWithRetractAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalMinWithRetractAggFunction(decimalTypeInfo)
        case TIME =>
          new TimeMinWithRetractAggFunction
        case DATE =>
          new DateMinWithRetractAggFunction
        case TIMESTAMP =>
          new TimestampMinWithRetractAggFunction
        case t: InternalType =>
          throw new TableException(s"Min with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0) match {
        case BYTE =>
          new MinAggFunction.ByteMinAggFunction
        case SHORT =>
          new MinAggFunction.ShortMinAggFunction
        case INT =>
          new MinAggFunction.IntMinAggFunction
        case LONG =>
          new MinAggFunction.LongMinAggFunction
        case FLOAT =>
          new MinAggFunction.FloatMinAggFunction
        case DOUBLE =>
          new MinAggFunction.DoubleMinAggFunction
        case BOOLEAN =>
          new MinAggFunction.BooleanMinAggFunction
        case STRING =>
          new MinAggFunction.StringMinAggFunction
        case DATE =>
          new MinAggFunction.DateMinAggFunction
        case TIME =>
          new MinAggFunction.TimeMinAggFunction
        case TIMESTAMP |  PROCTIME_INDICATOR | ROWTIME_INDICATOR =>
          new MinAggFunction.TimestampMinAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new MinAggFunction.DecimalMinAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"Min aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createLeadLagAggFunction(
      argTypes: Array[InternalType], index: Int): UserDefinedFunction = {
    argTypes(0) match {
      case BYTE =>
        new LeadLagAggFunction.ByteLeadLagAggFunction(argTypes.length)
      case SHORT =>
        new LeadLagAggFunction.ShortLeadLagAggFunction(argTypes.length)
      case INT =>
        new LeadLagAggFunction.IntLeadLagAggFunction(argTypes.length)
      case LONG =>
        new LeadLagAggFunction.LongLeadLagAggFunction(argTypes.length)
      case FLOAT =>
        new LeadLagAggFunction.FloatLeadLagAggFunction(argTypes.length)
      case DOUBLE =>
        new LeadLagAggFunction.DoubleLeadLagAggFunction(argTypes.length)
      case BOOLEAN =>
        new LeadLagAggFunction.BooleanLeadLagAggFunction(argTypes.length)
      case STRING =>
        new LeadLagAggFunction.StringLeadLagAggFunction(argTypes.length)
      case DATE =>
        new LeadLagAggFunction.DateLeadLagAggFunction(argTypes.length)
      case TIME =>
        new LeadLagAggFunction.TimeLeadLagAggFunction(argTypes.length)
      case TIMESTAMP | ROWTIME_INDICATOR | PROCTIME_INDICATOR =>
        new LeadLagAggFunction.TimestampLeadLagAggFunction(argTypes.length)
      case d: DecimalType =>
        new LeadLagAggFunction.DecimalLeadLagAggFunction(argTypes.length, d)
      case t: InternalType =>
        throw new TableException(s"LeadLag aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
    }
  }

  private def createMaxAggFunction(
      argTypes: Array[InternalType], index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0) match {
        case BYTE =>
          new ByteMaxWithRetractAggFunction
        case SHORT =>
          new ShortMaxWithRetractAggFunction
        case INT =>
          new IntMaxWithRetractAggFunction
        case LONG =>
          new LongMaxWithRetractAggFunction
        case FLOAT =>
          new FloatMaxWithRetractAggFunction
        case DOUBLE =>
          new DoubleMaxWithRetractAggFunction
        case BOOLEAN =>
          new BooleanMaxWithRetractAggFunction
        case STRING =>
          new StringMaxWithRetractAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalMaxWithRetractAggFunction(decimalTypeInfo)
        case TIME =>
          new TimeMaxWithRetractAggFunction
        case DATE =>
          new DateMaxWithRetractAggFunction
        case TIMESTAMP =>
          new TimestampMaxWithRetractAggFunction
        case t: InternalType =>
          throw new TableException(s"Max with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0) match {
        case BYTE =>
          new MaxAggFunction.ByteMaxAggFunction
        case SHORT =>
          new MaxAggFunction.ShortMaxAggFunction
        case INT =>
          new MaxAggFunction.IntMaxAggFunction
        case LONG =>
          new MaxAggFunction.LongMaxAggFunction
        case FLOAT =>
          new MaxAggFunction.FloatMaxAggFunction
        case DOUBLE =>
          new MaxAggFunction.DoubleMaxAggFunction
        case BOOLEAN =>
          new MaxAggFunction.BooleanMaxAggFunction
        case STRING =>
          new MaxAggFunction.StringMaxAggFunction
        case DATE =>
          new MaxAggFunction.DateMaxAggFunction
        case TIME =>
          new MaxAggFunction.TimeMaxAggFunction
        case TIMESTAMP | PROCTIME_INDICATOR | ROWTIME_INDICATOR =>
          new MaxAggFunction.TimestampMaxAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new MaxAggFunction.DecimalMaxAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"Max aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createCount1AggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    new Count1AggFunction
  }

  private def createCountAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    new CountAggFunction
  }
}
