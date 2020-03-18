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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.IncrSumAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.IncrSumWithRetractAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.SingleValueAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.SumWithRetractAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions._
import org.apache.flink.table.planner.functions.sql.{SqlFirstLastValueAggFunction, SqlListAggFunction}
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.{SqlAggFunction, SqlKind, SqlRankFunction}
import java.util

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

    val argTypes: Array[LogicalType] = call.getArgList
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
      .toArray

    call.getAggregation match {
      case a: SqlAvgAggFunction if a.kind == SqlKind.AVG => createAvgAggFunction(argTypes)

      case _: SqlSumAggFunction => createSumAggFunction(argTypes, index)

      case _: SqlSumEmptyIsZeroAggFunction => createSum0AggFunction(argTypes)

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

      case _: SqlListAggFunction if call.getArgList.size() == 1 =>
        createListAggFunction(argTypes, index)

      case _: SqlListAggFunction if call.getArgList.size() == 2 =>
        createListAggWsFunction(argTypes, index)

      // TODO supports SqlCardinalityCountAggFunction

      case a: SqlAggFunction if a.getKind == SqlKind.COLLECT =>
        createCollectAggFunction(argTypes)

      case udagg: AggSqlFunction =>
        // Can not touch the literals, Calcite make them in previous RelNode.
        // In here, all inputs are input refs.
        val constants = new util.ArrayList[AnyRef]()
        argTypes.foreach(t => constants.add(null))
        udagg.makeFunction(
          constants.toArray,
          argTypes)

      case unSupported: SqlAggFunction =>
        throw new TableException(s"Unsupported Function: '${unSupported.getName}'")
    }
  }

  private def createAvgAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    argTypes(0).getTypeRoot match {
      case TINYINT =>
        new AvgAggFunction.ByteAvgAggFunction
      case SMALLINT =>
        new AvgAggFunction.ShortAvgAggFunction
      case INTEGER =>
        new AvgAggFunction.IntAvgAggFunction
      case BIGINT =>
        new AvgAggFunction.LongAvgAggFunction
      case FLOAT =>
        new AvgAggFunction.FloatAvgAggFunction
      case DOUBLE =>
        new AvgAggFunction.DoubleAvgAggFunction
      case DECIMAL =>
        val d = argTypes(0).asInstanceOf[DecimalType]
        new AvgAggFunction.DecimalAvgAggFunction(d)
      case t =>
        throw new TableException(s"Avg aggregate function does not support type: ''$t''.\n" +
          s"Please re-check the data type.")
    }
  }

  private def createSumAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteSumWithRetractAggFunction
        case SMALLINT =>
          new ShortSumWithRetractAggFunction
        case INTEGER =>
          new IntSumWithRetractAggFunction
        case BIGINT =>
          new LongSumWithRetractAggFunction
        case FLOAT =>
          new FloatSumWithRetractAggFunction
        case DOUBLE =>
          new DoubleSumWithRetractAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalSumWithRetractAggFunction(d)
        case t =>
          throw new TableException(s"Sum with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new SumAggFunction.ByteSumAggFunction
        case SMALLINT =>
          new SumAggFunction.ShortSumAggFunction
        case INTEGER =>
          new SumAggFunction.IntSumAggFunction
        case BIGINT =>
          new SumAggFunction.LongSumAggFunction
        case FLOAT =>
          new SumAggFunction.FloatSumAggFunction
        case DOUBLE =>
          new SumAggFunction.DoubleSumAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new SumAggFunction.DecimalSumAggFunction(d)
        case t =>
          throw new TableException(s"Sum aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createSum0AggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    argTypes(0).getTypeRoot match {
      case TINYINT =>
        new Sum0AggFunction.ByteSum0AggFunction
      case SMALLINT =>
        new Sum0AggFunction.ShortSum0AggFunction
      case INTEGER =>
        new Sum0AggFunction.IntSum0AggFunction
      case BIGINT =>
        new Sum0AggFunction.LongSum0AggFunction
      case FLOAT =>
        new Sum0AggFunction.FloatSum0AggFunction
      case DOUBLE =>
        new Sum0AggFunction.DoubleSum0AggFunction
      case DECIMAL =>
        val d = argTypes(0).asInstanceOf[DecimalType]
        new Sum0AggFunction.DecimalSum0AggFunction(d)
      case t =>
        throw new TableException(s"Sum0 aggregate function does not support type: ''$t''.\n" +
          s"Please re-check the data type.")
    }
  }

  private def createIncrSumAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteIncrSumWithRetractAggFunction
        case SMALLINT =>
          new ShortIncrSumWithRetractAggFunction
        case INTEGER =>
          new IntIncrSumWithRetractAggFunction
        case BIGINT =>
          new LongIncrSumWithRetractAggFunction
        case FLOAT =>
          new FloatIncrSumWithRetractAggFunction
        case DOUBLE =>
          new DoubleIncrSumWithRetractAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalIncrSumWithRetractAggFunction(d)
        case t =>
          throw new TableException(s"IncrSum with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteIncrSumAggFunction
        case SMALLINT =>
          new ShortIncrSumAggFunction
        case INTEGER =>
          new IntIncrSumAggFunction
        case BIGINT =>
          new LongIncrSumAggFunction
        case FLOAT =>
          new FloatIncrSumAggFunction
        case DOUBLE =>
          new DoubleIncrSumAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalIncrSumAggFunction(d)
        case t =>
          throw new TableException(s"IncrSum aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createMinAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteMinWithRetractAggFunction
        case SMALLINT =>
          new ShortMinWithRetractAggFunction
        case INTEGER =>
          new IntMinWithRetractAggFunction
        case BIGINT =>
          new LongMinWithRetractAggFunction
        case FLOAT =>
          new FloatMinWithRetractAggFunction
        case DOUBLE =>
          new DoubleMinWithRetractAggFunction
        case BOOLEAN =>
          new BooleanMinWithRetractAggFunction
        case VARCHAR | CHAR =>
          new StringMinWithRetractAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalMinWithRetractAggFunction(DecimalTypeInfo.of(d.getPrecision, d.getScale))
        case TIME_WITHOUT_TIME_ZONE =>
          new TimeMinWithRetractAggFunction
        case DATE =>
          new DateMinWithRetractAggFunction
        case TIMESTAMP_WITHOUT_TIME_ZONE =>
          val d = argTypes(0).asInstanceOf[TimestampType]
          new TimestampMinWithRetractAggFunction(d.getPrecision)
        case t =>
          throw new TableException(s"Min with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new MinAggFunction.ByteMinAggFunction
        case SMALLINT =>
          new MinAggFunction.ShortMinAggFunction
        case INTEGER =>
          new MinAggFunction.IntMinAggFunction
        case BIGINT =>
          new MinAggFunction.LongMinAggFunction
        case FLOAT =>
          new MinAggFunction.FloatMinAggFunction
        case DOUBLE =>
          new MinAggFunction.DoubleMinAggFunction
        case BOOLEAN =>
          new MinAggFunction.BooleanMinAggFunction
        case VARCHAR | CHAR =>
          new MinAggFunction.StringMinAggFunction
        case DATE =>
          new MinAggFunction.DateMinAggFunction
        case TIME_WITHOUT_TIME_ZONE =>
          new MinAggFunction.TimeMinAggFunction
        case TIMESTAMP_WITHOUT_TIME_ZONE =>
          val d = argTypes(0).asInstanceOf[TimestampType]
          new MinAggFunction.TimestampMinAggFunction(d)
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new MinAggFunction.DecimalMinAggFunction(d)
        case t =>
          throw new TableException(s"Min aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createLeadLagAggFunction(
      argTypes: Array[LogicalType], index: Int): UserDefinedFunction = {
    argTypes(0).getTypeRoot match {
      case TINYINT =>
        new LeadLagAggFunction.ByteLeadLagAggFunction(argTypes.length)
      case SMALLINT =>
        new LeadLagAggFunction.ShortLeadLagAggFunction(argTypes.length)
      case INTEGER =>
        new LeadLagAggFunction.IntLeadLagAggFunction(argTypes.length)
      case BIGINT =>
        new LeadLagAggFunction.LongLeadLagAggFunction(argTypes.length)
      case FLOAT =>
        new LeadLagAggFunction.FloatLeadLagAggFunction(argTypes.length)
      case DOUBLE =>
        new LeadLagAggFunction.DoubleLeadLagAggFunction(argTypes.length)
      case BOOLEAN =>
        new LeadLagAggFunction.BooleanLeadLagAggFunction(argTypes.length)
      case VARCHAR =>
        new LeadLagAggFunction.StringLeadLagAggFunction(argTypes.length)
      case DATE =>
        new LeadLagAggFunction.DateLeadLagAggFunction(argTypes.length)
      case TIME_WITHOUT_TIME_ZONE =>
        new LeadLagAggFunction.TimeLeadLagAggFunction(argTypes.length)
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val d = argTypes(0).asInstanceOf[TimestampType]
        new LeadLagAggFunction.TimestampLeadLagAggFunction(argTypes.length, d)
      case DECIMAL =>
        val d = argTypes(0).asInstanceOf[DecimalType]
        new LeadLagAggFunction.DecimalLeadLagAggFunction(argTypes.length, d)
      case t =>
        throw new TableException(s"LeadLag aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
    }
  }

  private def createMaxAggFunction(
      argTypes: Array[LogicalType], index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteMaxWithRetractAggFunction
        case SMALLINT =>
          new ShortMaxWithRetractAggFunction
        case INTEGER =>
          new IntMaxWithRetractAggFunction
        case BIGINT =>
          new LongMaxWithRetractAggFunction
        case FLOAT =>
          new FloatMaxWithRetractAggFunction
        case DOUBLE =>
          new DoubleMaxWithRetractAggFunction
        case BOOLEAN =>
          new BooleanMaxWithRetractAggFunction
        case VARCHAR =>
          new StringMaxWithRetractAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalMaxWithRetractAggFunction(DecimalTypeInfo.of(d.getPrecision, d.getScale))
        case TIME_WITHOUT_TIME_ZONE =>
          new TimeMaxWithRetractAggFunction
        case DATE =>
          new DateMaxWithRetractAggFunction
        case TIMESTAMP_WITHOUT_TIME_ZONE =>
          val d = argTypes(0).asInstanceOf[TimestampType]
          new TimestampMaxWithRetractAggFunction(d.getPrecision)
        case t =>
          throw new TableException(s"Max with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new MaxAggFunction.ByteMaxAggFunction
        case SMALLINT =>
          new MaxAggFunction.ShortMaxAggFunction
        case INTEGER =>
          new MaxAggFunction.IntMaxAggFunction
        case BIGINT =>
          new MaxAggFunction.LongMaxAggFunction
        case FLOAT =>
          new MaxAggFunction.FloatMaxAggFunction
        case DOUBLE =>
          new MaxAggFunction.DoubleMaxAggFunction
        case BOOLEAN =>
          new MaxAggFunction.BooleanMaxAggFunction
        case VARCHAR =>
          new MaxAggFunction.StringMaxAggFunction
        case DATE =>
          new MaxAggFunction.DateMaxAggFunction
        case TIME_WITHOUT_TIME_ZONE =>
          new MaxAggFunction.TimeMaxAggFunction
        case TIMESTAMP_WITHOUT_TIME_ZONE =>
          val d = argTypes(0).asInstanceOf[TimestampType]
          new MaxAggFunction.TimestampMaxAggFunction(d)
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new MaxAggFunction.DecimalMaxAggFunction(d)
        case t =>
          throw new TableException(s"Max aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
      }
    }
  }

  private def createCount1AggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    new Count1AggFunction
  }

  private def createCountAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    new CountAggFunction
  }

  private def createSingleValueAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    argTypes(0).getTypeRoot match {
      case TINYINT =>
        new ByteSingleValueAggFunction
      case SMALLINT =>
        new ShortSingleValueAggFunction
      case INTEGER =>
        new IntSingleValueAggFunction
      case BIGINT =>
        new LongSingleValueAggFunction
      case FLOAT =>
        new FloatSingleValueAggFunction
      case DOUBLE =>
        new DoubleSingleValueAggFunction
      case BOOLEAN =>
        new BooleanSingleValueAggFunction
      case VARCHAR =>
        new StringSingleValueAggFunction
      case DATE =>
        new DateSingleValueAggFunction
      case TIME_WITHOUT_TIME_ZONE =>
        new TimeSingleValueAggFunction
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val d = argTypes(0).asInstanceOf[TimestampType]
        new TimestampSingleValueAggFunction(d)
      case DECIMAL =>
        val d = argTypes(0).asInstanceOf[DecimalType]
        new DecimalSingleValueAggFunction(d)
      case t =>
        throw new TableException(s"SINGLE_VALUE aggregate function doesn't support type '$t'.")
    }
  }

  private def createRowNumberAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    new RowNumberAggFunction
  }

  private def createRankAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    val argTypes = orderKeyIdx
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
    new RankAggFunction(argTypes)
  }

  private def createDenseRankAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    val argTypes = orderKeyIdx
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
    new DenseRankAggFunction(argTypes)
  }

  private def createFirstValueAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteFirstValueWithRetractAggFunction
        case SMALLINT =>
          new ShortFirstValueWithRetractAggFunction
        case INTEGER =>
          new IntFirstValueWithRetractAggFunction
        case BIGINT =>
          new LongFirstValueWithRetractAggFunction
        case FLOAT =>
          new FloatFirstValueWithRetractAggFunction
        case DOUBLE =>
          new DoubleFirstValueWithRetractAggFunction
        case BOOLEAN =>
          new BooleanFirstValueWithRetractAggFunction
        case VARCHAR =>
          new StringFirstValueWithRetractAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalFirstValueWithRetractAggFunction(
            DecimalTypeInfo.of(d.getPrecision, d.getScale))
        case t =>
          throw new TableException(s"FIRST_VALUE with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteFirstValueAggFunction
        case SMALLINT =>
          new ShortFirstValueAggFunction
        case INTEGER =>
          new IntFirstValueAggFunction
        case BIGINT =>
          new LongFirstValueAggFunction
        case FLOAT =>
          new FloatFirstValueAggFunction
        case DOUBLE =>
          new DoubleFirstValueAggFunction
        case BOOLEAN =>
          new BooleanFirstValueAggFunction
        case VARCHAR =>
          new StringFirstValueAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalFirstValueAggFunction(DecimalTypeInfo.of(d.getPrecision, d.getScale))
        case t =>
          throw new TableException(s"FIRST_VALUE aggregate function does not support " +
            s"type: ''$t''.\nPlease re-check the data type.")
      }
    }
  }

  private def createLastValueAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteLastValueWithRetractAggFunction
        case SMALLINT =>
          new ShortLastValueWithRetractAggFunction
        case INTEGER =>
          new IntLastValueWithRetractAggFunction
        case BIGINT =>
          new LongLastValueWithRetractAggFunction
        case FLOAT =>
          new FloatLastValueWithRetractAggFunction
        case DOUBLE =>
          new DoubleLastValueWithRetractAggFunction
        case BOOLEAN =>
          new BooleanLastValueWithRetractAggFunction
        case VARCHAR =>
          new StringLastValueWithRetractAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalLastValueWithRetractAggFunction(
            DecimalTypeInfo.of(d.getPrecision, d.getScale))
        case t =>
          throw new TableException(s"LAST_VALUE with retract aggregate function does not " +
            s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      argTypes(0).getTypeRoot match {
        case TINYINT =>
          new ByteLastValueAggFunction
        case SMALLINT =>
          new ShortLastValueAggFunction
        case INTEGER =>
          new IntLastValueAggFunction
        case BIGINT =>
          new LongLastValueAggFunction
        case FLOAT =>
          new FloatLastValueAggFunction
        case DOUBLE =>
          new DoubleLastValueAggFunction
        case BOOLEAN =>
          new BooleanLastValueAggFunction
        case VARCHAR =>
          new StringLastValueAggFunction
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new DecimalLastValueAggFunction(DecimalTypeInfo.of(d.getPrecision, d.getScale))
        case t =>
          throw new TableException(s"LAST_VALUE aggregate function does not support " +
            s"type: ''$t''.\nPlease re-check the data type.")
      }
    }
  }

  private def createListAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      new ListAggWithRetractAggFunction
    } else {
      new ListAggFunction(1)
    }
  }

  private def createListAggWsFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (needRetraction(index)) {
      new ListAggWsWithRetractAggFunction
    } else {
      new ListAggFunction(2)
    }
  }

  private def createCollectAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    val elementTypeInfo = argTypes(0) match {
      case gt: TypeInformationRawType[_] => gt.getTypeInformation
      case t => TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo(t)
    }
    new CollectAggFunction(elementTypeInfo)
  }
}
