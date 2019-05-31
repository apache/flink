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
import org.apache.flink.table.`type`.{DecimalType, GenericType, InternalType, TypeConverters}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.functions.aggfunctions.FirstValueAggFunction._
import org.apache.flink.table.functions.aggfunctions.FirstValueWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.IncrSumAggFunction._
import org.apache.flink.table.functions.aggfunctions.IncrSumWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.LastValueAggFunction._
import org.apache.flink.table.functions.aggfunctions.LastValueWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.MaxWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.MinWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions.SingleValueAggFunction._
import org.apache.flink.table.functions.aggfunctions.SumWithRetractAggFunction._
import org.apache.flink.table.functions.aggfunctions._
import org.apache.flink.table.functions.sql.{SqlConcatAggFunction, SqlFirstLastValueAggFunction, SqlIncrSumAggFunction}
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.typeutils.DecimalTypeInfo

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.{SqlAggFunction, SqlKind, SqlRankFunction}

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

      case _: SqlIncrSumAggFunction => createIncrSumAggFunction(argTypes, index)

      case a: SqlMinMaxAggFunction if a.getKind == SqlKind.MIN =>
        createMinAggFunction(argTypes, index)

      case a: SqlMinMaxAggFunction if a.getKind == SqlKind.MAX =>
        createMaxAggFunction(argTypes, index)

      case _: SqlCountAggFunction if call.getArgList.size() > 1 =>
        throw new TableException("We now only support the count of one field.")

      // TODO supports ApproximateCountDistinctAggFunction and CountDistinctAggFunction

      case _: SqlCountAggFunction if call.getArgList.isEmpty => createCount1AggFunction(argTypes)

      case _: SqlCountAggFunction => createCountAggFunction(argTypes)

      case a: SqlRankFunction if a.getKind == SqlKind.ROW_NUMBER =>
        createRowNumberAggFunction(argTypes)

      case a: SqlRankFunction if a.getKind == SqlKind.RANK =>
        createRankAggFunction(argTypes)

      case a: SqlRankFunction if a.getKind == SqlKind.DENSE_RANK =>
        createDenseRankAggFunction(argTypes)

      case _: SqlLeadLagAggFunction =>
        createLeadLagAggFunction(argTypes, index)

      case _: SqlSingleValueAggFunction =>
        createSingleValueAggFunction(argTypes)

      case a: SqlFirstLastValueAggFunction if a.getKind == SqlKind.FIRST_VALUE =>
        createFirstValueAggFunction(argTypes, index)

      case a: SqlFirstLastValueAggFunction if a.getKind == SqlKind.LAST_VALUE =>
        createLastValueAggFunction(argTypes, index)

      case _: SqlConcatAggFunction if call.getArgList.size() == 1 =>
        createConcatAggFunction(argTypes, index)

      case _: SqlConcatAggFunction if call.getArgList.size() == 2 =>
        createConcatWsAggFunction(argTypes, index)

      // TODO supports SqlCardinalityCountAggFunction

      case a: SqlAggFunction if a.getKind == SqlKind.COLLECT =>
        createCollectAggFunction(argTypes)

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

  private def createIncrSumAggFunction(
      argTypes: Array[InternalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0) match {
        case BYTE =>
          new ByteIncrSumWithRetractAggFunction
        case SHORT =>
          new ShortIncrSumWithRetractAggFunction
        case INT =>
          new IntIncrSumWithRetractAggFunction
        case LONG =>
          new LongIncrSumWithRetractAggFunction
        case FLOAT =>
          new FloatIncrSumWithRetractAggFunction
        case DOUBLE =>
          new DoubleIncrSumWithRetractAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalIncrSumWithRetractAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"IncrSum with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0) match {
        case BYTE =>
          new ByteIncrSumAggFunction
        case SHORT =>
          new ShortIncrSumAggFunction
        case INT =>
          new IntIncrSumAggFunction
        case LONG =>
          new LongIncrSumAggFunction
        case FLOAT =>
          new FloatIncrSumAggFunction
        case DOUBLE =>
          new DoubleIncrSumAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalIncrSumAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"IncrSum aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
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
        case TIMESTAMP | PROCTIME_INDICATOR | ROWTIME_INDICATOR =>
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

  private def createSingleValueAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    argTypes(0) match {
      case BYTE =>
        new ByteSingleValueAggFunction
      case SHORT =>
        new ShortSingleValueAggFunction
      case INT =>
        new IntSingleValueAggFunction
      case LONG =>
        new LongSingleValueAggFunction
      case FLOAT =>
        new FloatSingleValueAggFunction
      case DOUBLE =>
        new DoubleSingleValueAggFunction
      case BOOLEAN =>
        new BooleanSingleValueAggFunction
      case STRING =>
        new StringSingleValueAggFunction
      case DATE =>
        new DateSingleValueAggFunction
      case TIME =>
        new TimeSingleValueAggFunction
      case TIMESTAMP | ROWTIME_INDICATOR | PROCTIME_INDICATOR =>
        new TimestampSingleValueAggFunction
      case d: DecimalType =>
        new DecimalSingleValueAggFunction(d.toTypeInfo)
      case t =>
        throw new TableException(s"SINGLE_VALUE aggregate function doesn't support type '$t'.")
    }
  }

  private def createRowNumberAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    new RowNumberAggFunction
  }

  private def createRankAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    val argTypes = orderKeyIdx
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toInternalType)
    new RankAggFunction(argTypes)
  }

  private def createDenseRankAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    val argTypes = orderKeyIdx
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toInternalType)
    new DenseRankAggFunction(argTypes)
  }

  private def createFirstValueAggFunction(
      argTypes: Array[InternalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0) match {
        case BYTE =>
          new ByteFirstValueWithRetractAggFunction
        case SHORT =>
          new ShortFirstValueWithRetractAggFunction
        case INT =>
          new IntFirstValueWithRetractAggFunction
        case LONG =>
          new LongFirstValueWithRetractAggFunction
        case FLOAT =>
          new FloatFirstValueWithRetractAggFunction
        case DOUBLE =>
          new DoubleFirstValueWithRetractAggFunction
        case BOOLEAN =>
          new BooleanFirstValueWithRetractAggFunction
        case STRING =>
          new StringFirstValueWithRetractAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalFirstValueWithRetractAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"FIRST_VALUE with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0) match {
        case BYTE =>
          new ByteFirstValueAggFunction
        case SHORT =>
          new ShortFirstValueAggFunction
        case INT =>
          new IntFirstValueAggFunction
        case LONG =>
          new LongFirstValueAggFunction
        case FLOAT =>
          new FloatFirstValueAggFunction
        case DOUBLE =>
          new DoubleFirstValueAggFunction
        case BOOLEAN =>
          new BooleanFirstValueAggFunction
        case STRING =>
          new StringFirstValueAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalFirstValueAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"FIRST_VALUE aggregate function does not support " +
            s"type: ''$t''.\nPlease re-check the data type.")
      }
    }
  }

  private def createLastValueAggFunction(
      argTypes: Array[InternalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0) match {
        case BYTE =>
          new ByteLastValueWithRetractAggFunction
        case SHORT =>
          new ShortLastValueWithRetractAggFunction
        case INT =>
          new IntLastValueWithRetractAggFunction
        case LONG =>
          new LongLastValueWithRetractAggFunction
        case FLOAT =>
          new FloatLastValueWithRetractAggFunction
        case DOUBLE =>
          new DoubleLastValueWithRetractAggFunction
        case BOOLEAN =>
          new BooleanLastValueWithRetractAggFunction
        case STRING =>
          new StringLastValueWithRetractAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalLastValueWithRetractAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"LAST_VALUE with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0) match {
        case BYTE =>
          new ByteLastValueAggFunction
        case SHORT =>
          new ShortLastValueAggFunction
        case INT =>
          new IntLastValueAggFunction
        case LONG =>
          new LongLastValueAggFunction
        case FLOAT =>
          new FloatLastValueAggFunction
        case DOUBLE =>
          new DoubleLastValueAggFunction
        case BOOLEAN =>
          new BooleanLastValueAggFunction
        case STRING =>
          new StringLastValueAggFunction
        case d: DecimalType =>
          val decimalTypeInfo = DecimalTypeInfo.of(d.precision(), d.scale())
          new DecimalLastValueAggFunction(decimalTypeInfo)
        case t: InternalType =>
          throw new TableException(s"LAST_VALUE aggregate function does not support " +
            s"type: ''$t''.\nPlease re-check the data type.")
      }
    }
  }

  private def createConcatAggFunction(
      argTypes: Array[InternalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      new ConcatWithRetractAggFunction
    } else {
      new ConcatAggFunction(1)
    }
  }

  private def createConcatWsAggFunction(
      argTypes: Array[InternalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      new ConcatWsWithRetractAggFunction
    } else {
      new ConcatAggFunction(2)
    }
  }

  private def createCollectAggFunction(argTypes: Array[InternalType]): UserDefinedFunction = {
    val elementTypeInfo = argTypes(0) match {
      case gt: GenericType[_] => gt.getTypeInfo
      case t => TypeConverters.createExternalTypeInfoFromInternalType(t)
    }
    new CollectAggFunction(elementTypeInfo)
  }
}
