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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.{SqlAggFunction, SqlKind, SqlRankFunction}
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes._
import org.apache.flink.table.api.types.{DataType, DecimalType, InternalType}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.runtime.functions.aggfunctions.ApproximateCountDistinct._
import org.apache.flink.table.runtime.functions.aggfunctions.CountDistinct._
import org.apache.flink.table.runtime.functions.aggfunctions._
import org.apache.flink.table.codegen.expr.RowNumberFunction
import org.apache.flink.table.functions.sql.{SqlCardinalityCountAggFunction, SqlConcatAggFunction, SqlFirstLastValueAggFunction, SqlIncrSumAggFunction, SqlMax2ndAggFunction}
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.runtime.functions.aggfunctions.CardinalityCountAggFunction
import org.apache.flink.table.typeutils.{BinaryStringTypeInfo, DecimalTypeInfo}

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
    
    val argTypes: Array[DataType] = call.getArgList
      .map(inputType.getFieldList.get(_).getType) // RelDataType
      .map(FlinkTypeFactory.toDataType)       // InternalType
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

      case _: SqlCountAggFunction if call.isApproximate && call.isDistinct =>
        createApproximateCountDistinctAggFunction(argTypes)

      case _: SqlCountAggFunction if call.isDistinct => createCountDistinctAggFunction(argTypes)

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

      case _: SqlMax2ndAggFunction =>
        createMax2ndAggFunction(argTypes, index)

      case _: SqlSingleValueAggFunction => createSingleValueAggFunction(argTypes)

      case a: SqlFirstLastValueAggFunction if a.getKind == SqlKind.FIRST_VALUE =>
        createFirstValueAggFunction(argTypes, index)

      case a: SqlFirstLastValueAggFunction if a.getKind == SqlKind.LAST_VALUE =>
        createLastValueAggFunction(argTypes, index)

      case _: SqlConcatAggFunction if call.getArgList.size() == 1 =>
        createConcatAggFunction(argTypes, index)

      case _: SqlConcatAggFunction if call.getArgList.size() == 2 =>
        createConcatWsAggFunction(argTypes, index)

      case _: SqlCardinalityCountAggFunction => createCardinalityCountAggFunction(argTypes)

      case collect: SqlAggFunction if collect.getKind == SqlKind.COLLECT =>
        createCollectAggFunction(argTypes)

      case udagg: AggSqlFunction => udagg.getFunction

      case unSupported: SqlAggFunction =>
        throw new TableException(s"unsupported Function: '${unSupported.getName}'")

    }
  }

  private def createAvgAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    argTypes(0).toInternalType match {
      case BYTE | SHORT | INT | LONG =>
        new org.apache.flink.table.codegen.expr.IntegralAvgAggFunction
      case FLOAT | DOUBLE =>
        new org.apache.flink.table.codegen.expr.DoubleAvgAggFunction
      case d: DecimalType =>
        new org.apache.flink.table.codegen.expr.DecimalAvgAggFunction(d)
      case t: DataType =>
        throw new TableException(
          TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Avg", t.toString))
    }
  }

  private def createSumAggFunction(argTypes: Array[DataType], index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).toInternalType match {
        case BYTE =>
          new org.apache.flink.table.codegen.expr.ByteSumWithRetractAggFunction
        case SHORT =>
          new org.apache.flink.table.codegen.expr.ShortSumWithRetractAggFunction
        case INT =>
          new org.apache.flink.table.codegen.expr.IntSumWithRetractAggFunction
        case LONG =>
          new org.apache.flink.table.codegen.expr.LongSumWithRetractAggFunction
        case FLOAT =>
          new org.apache.flink.table.codegen.expr.FloatSumWithRetractAggFunction
        case DOUBLE =>
          new org.apache.flink.table.codegen.expr.DoubleSumWithRetractAggFunction
        case d: DecimalType =>
          new org.apache.flink.table.codegen.expr.DecimalSumWithRetractAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Sum", t.toString))
      }
    } else {
      argTypes(0).toInternalType match {
        case BYTE =>
          new org.apache.flink.table.codegen.expr.ByteSumAggFunction
        case SHORT =>
          new org.apache.flink.table.codegen.expr.ShortSumAggFunction
        case INT =>
          new org.apache.flink.table.codegen.expr.IntSumAggFunction
        case LONG =>
          new org.apache.flink.table.codegen.expr.LongSumAggFunction
        case FLOAT =>
          new org.apache.flink.table.codegen.expr.FloatSumAggFunction
        case DOUBLE =>
          new org.apache.flink.table.codegen.expr.DoubleSumAggFunction
        case d: DecimalType =>
          new org.apache.flink.table.codegen.expr.DecimalSumAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Sum", t.toString))
      }
    }
  }

  private def createIncrSumAggFunction(argTypes: Array[DataType], index: Int)
    : UserDefinedFunction = {
    if (needRetraction(index)) {
       argTypes(0).toInternalType match {
         case BYTE =>
           new org.apache.flink.table.codegen.expr.ByteIncrSumWithRetractAggFunction
         case SHORT =>
           new org.apache.flink.table.codegen.expr.ShortIncrSumWithRetractAggFunction
         case INT =>
           new org.apache.flink.table.codegen.expr.IntIncrSumWithRetractAggFunction
         case LONG =>
           new org.apache.flink.table.codegen.expr.LongIncrSumWithRetractAggFunction
         case FLOAT =>
           new org.apache.flink.table.codegen.expr.FloatIncrSumWithRetractAggFunction
         case DOUBLE =>
           new org.apache.flink.table.codegen.expr.DoubleIncrSumWithRetractAggFunction
         case d: DecimalType =>
           new org.apache.flink.table.codegen.expr.DecimalIncrSumWithRetractAggFunction(d)
         case t: DataType =>
           throw new TableException(
             TableErrors.INST.sqlAggFunctionDataTypeNotSupported("IncrSum", t.toString))
       }
    } else {
       argTypes(0).toInternalType match {
        case BYTE =>
          new org.apache.flink.table.codegen.expr.ByteIncrSumAggFunction
        case SHORT =>
          new org.apache.flink.table.codegen.expr.ShortIncrSumAggFunction
        case INT =>
          new org.apache.flink.table.codegen.expr.IntIncrSumAggFunction
        case LONG =>
          new org.apache.flink.table.codegen.expr.LongIncrSumAggFunction
        case FLOAT =>
          new org.apache.flink.table.codegen.expr.FloatIncrSumAggFunction
        case DOUBLE =>
          new org.apache.flink.table.codegen.expr.DoubleIncrSumAggFunction
        case d: DecimalType =>
          new org.apache.flink.table.codegen.expr.DecimalIncrSumAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("IncrSum", t.toString))
      }
    }
  }

  private def createSum0AggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    argTypes(0).toInternalType match {
      case BYTE =>
        new org.apache.flink.table.codegen.expr.ByteSum0AggFunction
      case SHORT =>
        new org.apache.flink.table.codegen.expr.ShortSum0AggFunction
      case INT =>
        new org.apache.flink.table.codegen.expr.IntSum0AggFunction
      case LONG =>
        new org.apache.flink.table.codegen.expr.LongSum0AggFunction
      case FLOAT =>
        new org.apache.flink.table.codegen.expr.FloatSum0AggFunction
      case DOUBLE =>
        new org.apache.flink.table.codegen.expr.DoubleSum0AggFunction
      case d: DecimalType =>
        new org.apache.flink.table.codegen.expr.DecimalSum0AggFunction(d)
      case t: DataType =>
        throw new TableException(
          TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Sum0", t.toString))
    }
  }

  private def createMinAggFunction(argTypes: Array[DataType], index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).toInternalType match {
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
          new DecimalMinWithRetractAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Min with retract", t.toString))
      }
    } else {
      argTypes(0).toInternalType match {
        case BYTE =>
          new org.apache.flink.table.codegen.expr.ByteMinAggFunction
        case SHORT =>
          new org.apache.flink.table.codegen.expr.ShortMinAggFunction
        case INT =>
          new org.apache.flink.table.codegen.expr.IntMinAggFunction
        case LONG =>
          new org.apache.flink.table.codegen.expr.LongMinAggFunction
        case FLOAT =>
          new org.apache.flink.table.codegen.expr.FloatMinAggFunction
        case DOUBLE =>
          new org.apache.flink.table.codegen.expr.DoubleMinAggFunction
        case BOOLEAN =>
          new org.apache.flink.table.codegen.expr.BooleanMinAggFunction
        case STRING =>
          new org.apache.flink.table.codegen.expr.StringMinAggFunction
        case DATE =>
          new org.apache.flink.table.codegen.expr.DateMinAggFunction
        case TIME =>
          new org.apache.flink.table.codegen.expr.TimeMinAggFunction
        case TIMESTAMP | ROWTIME_INDICATOR | PROCTIME_INDICATOR =>
          new org.apache.flink.table.codegen.expr.TimestampMinAggFunction
        case d: DecimalType =>
          new org.apache.flink.table.codegen.expr.DecimalMinAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Min", t.toString))
      }
    }
  }

  private def createLeadLagAggFunction(
      argTypes: Array[DataType], index: Int): UserDefinedFunction = {
      argTypes(0).toInternalType match {
      case BYTE =>
        new org.apache.flink.table.codegen.expr.ByteLeadLagAggFunction(argTypes.length)
      case SHORT =>
        new org.apache.flink.table.codegen.expr.ShortLeadLagAggFunction(argTypes.length)
      case INT =>
        new org.apache.flink.table.codegen.expr.IntLeadLagAggFunction(argTypes.length)
      case LONG =>
        new org.apache.flink.table.codegen.expr.LongLeadLagAggFunction(argTypes.length)
      case FLOAT =>
        new org.apache.flink.table.codegen.expr.FloatLeadLagAggFunction(argTypes.length)
      case DOUBLE =>
        new org.apache.flink.table.codegen.expr.DoubleLeadLagAggFunction(argTypes.length)
      case BOOLEAN =>
        new org.apache.flink.table.codegen.expr.BooleanLeadLagAggFunction(argTypes.length)
      case STRING =>
        new org.apache.flink.table.codegen.expr.StringLeadLagAggFunction(argTypes.length)
      case DATE =>
        new org.apache.flink.table.codegen.expr.DateLeadLagAggFunction(argTypes.length)
      case TIME =>
        new org.apache.flink.table.codegen.expr.TimeLeadLagAggFunction(argTypes.length)
      case TIMESTAMP | ROWTIME_INDICATOR | PROCTIME_INDICATOR =>
        new org.apache.flink.table.codegen.expr.TimestampLeadLagAggFunction(argTypes.length)
      case d: DecimalType =>
        new org.apache.flink.table.codegen.expr.DecimalLeadLagAggFunction(argTypes.length, d)
      case t: DataType =>
        throw new TableException(
          TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Leag/Lag", t.toString))
      }
  }

  private def createMaxAggFunction(argTypes: Array[DataType], index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).toInternalType match {
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
          new DecimalMaxWithRetractAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Max with retract", t.toString))
      }
    } else {
      argTypes(0).toInternalType match {
        case BYTE =>
          new org.apache.flink.table.codegen.expr.ByteMaxAggFunction
        case SHORT =>
          new org.apache.flink.table.codegen.expr.ShortMaxAggFunction
        case INT =>
          new org.apache.flink.table.codegen.expr.IntMaxAggFunction
        case LONG =>
          new org.apache.flink.table.codegen.expr.LongMaxAggFunction
        case FLOAT =>
          new org.apache.flink.table.codegen.expr.FloatMaxAggFunction
        case DOUBLE =>
          new org.apache.flink.table.codegen.expr.DoubleMaxAggFunction
        case BOOLEAN =>
          new org.apache.flink.table.codegen.expr.BooleanMaxAggFunction
        case STRING =>
          new org.apache.flink.table.codegen.expr.StringMaxAggFunction
        case DATE =>
          new org.apache.flink.table.codegen.expr.DateMaxAggFunction
        case TIME =>
          new org.apache.flink.table.codegen.expr.TimeMaxAggFunction
        case TIMESTAMP | ROWTIME_INDICATOR | PROCTIME_INDICATOR =>
          new org.apache.flink.table.codegen.expr.TimestampMaxAggFunction
        case d: DecimalType =>
          new org.apache.flink.table.codegen.expr.DecimalMaxAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Max", t.toString))
      }
    }
  }

  private def createCountDistinctAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    argTypes(0).toInternalType match {
      case BYTE =>
        new ByteCountDistinctAggFunction
      case SHORT =>
        new ShortCountDistinctAggFunction
      case INT =>
        new IntCountDistinctAggFunction
      case LONG =>
        new LongCountDistinctAggFunction
      case FLOAT =>
        new FloatCountDistinctAggFunction
      case DOUBLE =>
        new DoubleCountDistinctAggFunction
      case BOOLEAN =>
        new BooleanCountDistinctAggFunction
      case DATE =>
        new DateCountDistinctAggFunction
      case TIME =>
        new TimeCountDistinctAggFunction
      case TIMESTAMP =>
        new TimestampCountDistinctAggFunction
      case STRING =>
        new StringCountDistinctAggFunction
      case d: DecimalType =>
        new DecimalCountDistinctAggFunction(d)
      case t =>
        throw new TableException(
          TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Count Distinct", t.toString))
    }
  }

  private def createApproximateCountDistinctAggFunction(argTypes: Array[DataType]):
  UserDefinedFunction = {
    argTypes(0).toInternalType match {
      case BYTE =>
        new ByteApproximateCountDistinctAggFunction
      case SHORT =>
        new ShortApproximateCountDistinctAggFunction
      case INT =>
        new IntApproximateCountDistinctAggFunction
      case LONG =>
        new LongApproximateCountDistinctAggFunction
      case FLOAT =>
        new FloatApproximateCountDistinctAggFunction
      case DOUBLE =>
        new DoubleApproximateCountDistinctAggFunction
      case BOOLEAN =>
        new BooleanApproximateCountDistinctAggFunction
      case DATE =>
        new DateApproximateCountDistinctAggFunction
      case TIME =>
        new TimeApproximateCountDistinctAggFunction
      case TIMESTAMP =>
        new TimestampApproximateCountDistinctAggFunction
      case STRING =>
        new StringApproximateCountDistinctAggFunction
      case d: DecimalType =>
        new DecimalApproximateCountDistinctAggFunction(d)
      case t =>
        throw new TableException(
          TableErrors.INST.sqlAggFunctionDataTypeNotSupported(
            "Approximate Count Distinct", t.toString))
    }
  }

  private def createCount1AggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    new org.apache.flink.table.codegen.expr.Count1AggFunction
  }

  private def createCountAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    new org.apache.flink.table.codegen.expr.CountAggFunction
  }

  private def createRowNumberAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    new RowNumberFunction
  }

  private def createRankAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    val argTypes = orderKeyIdx
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toInternalType)
    new org.apache.flink.table.codegen.expr.RankFunction(argTypes)
  }

  private def createDenseRankAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    val argTypes = orderKeyIdx
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toInternalType)
    new org.apache.flink.table.codegen.expr.DenseRankFunction(argTypes)
  }

  private def createMax2ndAggFunction(argTypes: Array[DataType], index: Int):
  UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).toInternalType match {
        case BYTE =>
          new ByteMax2ndWithRetractAggFunction
        case SHORT =>
          new ShortMax2ndWithRetractAggFunction
        case INT =>
          new IntMax2ndWithRetractAggFunction
        case LONG =>
          new LongMax2ndWithRetractAggFunction
        case FLOAT =>
          new FloatMax2ndWithRetractAggFunction
        case DOUBLE =>
          new DoubleMax2ndWithRetractAggFunction
        case BOOLEAN =>
          new BooleanMax2ndWithRetractAggFunction
        case STRING =>
          new StringMax2ndWithRetractAggFunction
        case d: DecimalType =>
          new DecimalMax2ndWithRetractAggFunction(d)
        case t: InternalType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Max2nd with retract", t.toString))
      }
    } else {
      argTypes(0).toInternalType match {
        case BYTE =>
          new ByteMax2ndAggFunction
        case SHORT =>
          new ShortMax2ndAggFunction
        case INT =>
          new IntMax2ndAggFunction
        case LONG =>
          new LongMax2ndAggFunction
        case FLOAT =>
          new FloatMax2ndAggFunction
        case DOUBLE =>
          new DoubleMax2ndAggFunction
        case BOOLEAN =>
          new BooleanMax2ndAggFunction
        case STRING =>
          new StringMax2ndAggFunction
        case d: DecimalType =>
          new DecimalMax2ndAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("Max2nd", t.toString))
      }
    }
  }

  private def createSingleValueAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    argTypes(0).toInternalType match {
      case BYTE =>
        new org.apache.flink.table.codegen.expr.ByteSingleValueAggFunction
      case SHORT =>
        new org.apache.flink.table.codegen.expr.ShortSingleValueAggFunction
      case INT =>
        new org.apache.flink.table.codegen.expr.IntSingleValueAggFunction
      case LONG =>
        new org.apache.flink.table.codegen.expr.LongSingleValueAggFunction
      case FLOAT =>
        new org.apache.flink.table.codegen.expr.FloatSingleValueAggFunction
      case DOUBLE =>
        new org.apache.flink.table.codegen.expr.DoubleSingleValueAggFunction
      case BOOLEAN =>
        new org.apache.flink.table.codegen.expr.BooleanSingleValueAggFunction
      case STRING =>
        new org.apache.flink.table.codegen.expr.StringSingleValueAggFunction
      case DATE =>
        new org.apache.flink.table.codegen.expr.DateSingleValueAggFunction
      case TIME =>
        new org.apache.flink.table.codegen.expr.TimeSingleValueAggFunction
      case TIMESTAMP | ROWTIME_INDICATOR | PROCTIME_INDICATOR =>
        new org.apache.flink.table.codegen.expr.TimestampSingleValueAggFunction
      case d: DecimalType =>
        new org.apache.flink.table.codegen.expr.DecimalSingleValueAggFunction(d)
      case t: DataType =>
        throw new TableException(
          TableErrors.INST.sqlAggFunctionDataTypeNotSupported("SINGLE_VALUE", t.toString))
    }
  }

  private def createFirstValueAggFunction(argTypes: Array[DataType], index: Int):
  UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).toInternalType match {
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
          new DecimalFirstValueWithRetractAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("FIRST_VALUE", t.toString))
      }
    } else {
      argTypes(0).toInternalType match {
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
          new DecimalFirstValueAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("FIRST_VALUE", t.toString))
      }
    }
  }

  private def createLastValueAggFunction(argTypes: Array[DataType], index: Int):
  UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).toInternalType match {
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
          new DecimalLastValueWithRetractAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("LAST_VALUE", t.toString))
      }
    } else {
      argTypes(0).toInternalType match {
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
          new DecimalLastValueAggFunction(d)
        case t: DataType =>
          throw new TableException(
            TableErrors.INST.sqlAggFunctionDataTypeNotSupported("LAST_VALUE", t.toString))
      }
    }
  }

  private def createConcatAggFunction(argTypes: Array[DataType], index: Int):
  UserDefinedFunction = {
    if (needRetraction(index)) {
      new ConcatAggFunction
    } else {
      new org.apache.flink.table.codegen.expr.ConcatAggFunction(1)
    }
  }

  private def createConcatWsAggFunction(argTypes: Array[DataType], index: Int):
  UserDefinedFunction = {
    if (needRetraction(index)) {
      new ConcatWsAggFunction
    } else {
      new org.apache.flink.table.codegen.expr.ConcatAggFunction(2)
    }
  }

  private def createCardinalityCountAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    new CardinalityCountAggFunction
  }

  private def createCollectAggFunction(argTypes: Array[DataType]): UserDefinedFunction = {
    argTypes(0).toInternalType match {
      case STRING =>
        new CollectAggFunction(BinaryStringTypeInfo.INSTANCE)
      case d: DecimalType =>
        new CollectAggFunction(DecimalTypeInfo.of(d.precision(), d.scale()))
      case t: DataType =>
        new CollectAggFunction(t)
    }
  }
}
