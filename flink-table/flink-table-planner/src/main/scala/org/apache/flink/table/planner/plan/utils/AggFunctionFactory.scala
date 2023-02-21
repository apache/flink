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
import org.apache.flink.table.functions.{DeclarativeAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.planner.functions.aggfunctions._
import org.apache.flink.table.planner.functions.aggfunctions.SingleValueAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.SumWithRetractAggFunction._
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction
import org.apache.flink.table.planner.functions.sql.{SqlFirstLastValueAggFunction, SqlListAggFunction}
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.runtime.functions.aggregate.{BuiltInAggregateFunction, CollectAggFunction, FirstValueAggFunction, FirstValueWithRetractAggFunction, JsonArrayAggFunction, JsonObjectAggFunction, LagAggFunction, LastValueAggFunction, LastValueWithRetractAggFunction, ListAggWithRetractAggFunction, ListAggWsWithRetractAggFunction, MaxWithRetractAggFunction, MinWithRetractAggFunction}
import org.apache.flink.table.runtime.functions.aggregate.BatchApproxCountDistinctAggFunctions._
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.{SqlAggFunction, SqlJsonConstructorNullClause, SqlKind, SqlRankFunction}
import org.apache.calcite.sql.fun._

import java.util

import scala.collection.JavaConversions._

/**
 * Factory for creating runtime implementation for internal aggregate functions that are declared as
 * subclasses of [[SqlAggFunction]] in Calcite but not as [[BridgingSqlAggFunction]]. The factory
 * returns [[DeclarativeAggregateFunction]] or [[BuiltInAggregateFunction]].
 *
 * @param inputRowType
 *   the input row type
 * @param orderKeyIndexes
 *   the indexes of order key (null when is not over agg)
 * @param aggCallNeedRetractions
 *   true if need retraction
 * @param isBounded
 *   true if the source is bounded source
 */
class AggFunctionFactory(
    inputRowType: RowType,
    orderKeyIndexes: Array[Int],
    aggCallNeedRetractions: Array[Boolean],
    isBounded: Boolean) {

  /** The entry point to create an aggregate function from the given [[AggregateCall]]. */
  def createAggFunction(call: AggregateCall, index: Int): UserDefinedFunction = {

    val argTypes: Array[LogicalType] = call.getArgList
      .map(inputRowType.getChildren.get(_))
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

      // TODO supports CountDistinctAggFunction
      case _: SqlCountAggFunction if call.isDistinct && call.isApproximate =>
        createApproxCountDistinctAggFunction(argTypes, index)

      case _: SqlCountAggFunction if call.getArgList.isEmpty => createCount1AggFunction(argTypes)

      case _: SqlCountAggFunction => createCountAggFunction(argTypes)

      case a: SqlRankFunction if a.getKind == SqlKind.ROW_NUMBER =>
        createRowNumberAggFunction(argTypes)

      case a: SqlRankFunction if a.getKind == SqlKind.RANK =>
        createRankAggFunction(argTypes)

      case a: SqlRankFunction if a.getKind == SqlKind.DENSE_RANK =>
        createDenseRankAggFunction(argTypes)

      case a: SqlRankFunction if a.getKind == SqlKind.CUME_DIST =>
        if (isBounded) {
          createCumeDistAggFunction(argTypes)
        } else {
          throw new TableException("CUME_DIST Function is not supported in stream mode.")
        }

      case a: SqlRankFunction if a.getKind == SqlKind.PERCENT_RANK =>
        if (isBounded) {
          createPercentRankAggFunction(argTypes)
        } else {
          throw new TableException("PERCENT_RANK Function is not supported in stream mode.")
        }

      case _: SqlNtileAggFunction =>
        if (isBounded) {
          createNTILEAggFUnction(argTypes)
        } else {
          throw new TableException("NTILE Function is not supported in stream mode.")
        }

      case func: SqlLeadLagAggFunction =>
        if (isBounded) {
          createBatchLeadLagAggFunction(argTypes, index)
        } else {
          createStreamLeadLagAggFunction(func, argTypes, index)
        }

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

      case fn: SqlAggFunction if fn.getKind == SqlKind.JSON_OBJECTAGG =>
        val onNull = fn.asInstanceOf[SqlJsonObjectAggAggFunction].getNullClause
        new JsonObjectAggFunction(argTypes, onNull == SqlJsonConstructorNullClause.ABSENT_ON_NULL)

      case fn: SqlAggFunction if fn.getKind == SqlKind.JSON_ARRAYAGG =>
        val onNull = fn.asInstanceOf[SqlJsonArrayAggAggFunction].getNullClause
        new JsonArrayAggFunction(argTypes, onNull == SqlJsonConstructorNullClause.ABSENT_ON_NULL)

      case udagg: AggSqlFunction =>
        // Can not touch the literals, Calcite make them in previous RelNode.
        // In here, all inputs are input refs.
        val constants = new util.ArrayList[AnyRef]()
        argTypes.foreach(_ => constants.add(null))
        udagg.makeFunction(constants.toArray, argTypes)

      case bridge: BridgingSqlAggFunction =>
        bridge.getDefinition.asInstanceOf[UserDefinedFunction]

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
        throw new TableException(
          s"Avg aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
    }
  }

  private def createSumAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (aggCallNeedRetractions(index)) {
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
          throw new TableException(
            s"Sum with retract aggregate function does not " +
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
          throw new TableException(
            s"Sum aggregate function does not support type: ''$t''.\n" +
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
        throw new TableException(
          s"Sum0 aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
    }
  }

  private def createMinAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    val valueType = argTypes(0)
    if (aggCallNeedRetractions(index)) {
      valueType.getTypeRoot match {
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | VARCHAR | DECIMAL |
            TIME_WITHOUT_TIME_ZONE | DATE | TIMESTAMP_WITHOUT_TIME_ZONE |
            TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
          new MinWithRetractAggFunction(argTypes(0))
        case t =>
          throw new TableException(
            s"Min with retract aggregate function does not " +
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
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
          val ltzType = argTypes(0).asInstanceOf[LocalZonedTimestampType]
          new MinAggFunction.TimestampLtzMinAggFunction(ltzType)
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new MinAggFunction.DecimalMinAggFunction(d)
        case t =>
          throw new TableException(
            s"Min aggregate function does not support type: ''$t''.\n" +
              s"Please re-check the data type.")
      }
    }
  }

  private def createStreamLeadLagAggFunction(
      func: SqlLeadLagAggFunction,
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (func.getKind == SqlKind.LEAD) {
      throw new TableException("LEAD Function is not supported in stream mode.")
    }

    if (aggCallNeedRetractions(index)) {
      throw new TableException("LAG Function with retraction is not supported in stream mode.")
    }

    new LagAggFunction(argTypes)
  }

  private def createBatchLeadLagAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
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
      case CHAR =>
        val d = argTypes(0).asInstanceOf[CharType]
        new LeadLagAggFunction.CharLeadLagAggFunction(argTypes.length, d);
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
        throw new TableException(
          s"LeadLag aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
    }
  }

  private def createMaxAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    val valueType = argTypes(0)
    if (aggCallNeedRetractions(index)) {
      valueType.getTypeRoot match {
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | VARCHAR | DECIMAL |
            TIME_WITHOUT_TIME_ZONE | DATE | TIMESTAMP_WITHOUT_TIME_ZONE |
            TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
          new MaxWithRetractAggFunction(argTypes(0))
        case t =>
          throw new TableException(
            s"Max with retract aggregate function does not " +
              s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      valueType.getTypeRoot match {
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
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
          val ltzType = argTypes(0).asInstanceOf[LocalZonedTimestampType]
          new MaxAggFunction.TimestampLtzMaxAggFunction(ltzType)
        case DECIMAL =>
          val d = argTypes(0).asInstanceOf[DecimalType]
          new MaxAggFunction.DecimalMaxAggFunction(d)
        case t =>
          throw new TableException(
            s"Max aggregate function does not support type: ''$t''.\n" +
              s"Please re-check the data type.")
      }
    }
  }

  private def createApproxCountDistinctAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (!isBounded) {
      throw new TableException(
        s"APPROX_COUNT_DISTINCT aggregate function does not support yet for streaming.")
    }
    argTypes(0).getTypeRoot match {
      case TINYINT =>
        new ByteApproxCountDistinctAggFunction
      case SMALLINT =>
        new ShortApproxCountDistinctAggFunction
      case INTEGER =>
        new IntApproxCountDistinctAggFunction
      case BIGINT =>
        new LongApproxCountDistinctAggFunction
      case FLOAT =>
        new FloatApproxCountDistinctAggFunction
      case DOUBLE =>
        new DoubleApproxCountDistinctAggFunction
      case DATE =>
        new DateApproxCountDistinctAggFunction
      case TIME_WITHOUT_TIME_ZONE =>
        new TimeApproxCountDistinctAggFunction
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val d = argTypes(0).asInstanceOf[TimestampType]
        new TimestampApproxCountDistinctAggFunction(d)
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val ltzType = argTypes(0).asInstanceOf[LocalZonedTimestampType]
        new TimestampLtzApproxCountDistinctAggFunction(ltzType)
      case DECIMAL =>
        val d = argTypes(0).asInstanceOf[DecimalType]
        new DecimalApproxCountDistinctAggFunction(d)
      case CHAR | VARCHAR =>
        new StringApproxCountDistinctAggFunction()

      case t =>
        throw new TableException(
          s"APPROX_COUNT_DISTINCT aggregate function does not support type: ''$t''.\n" +
            s"Please re-check the data type.")
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
      case CHAR =>
        val d = argTypes(0).asInstanceOf[CharType]
        new CharSingleValueAggFunction(d)
      case VARCHAR =>
        new StringSingleValueAggFunction
      case DATE =>
        new DateSingleValueAggFunction
      case TIME_WITHOUT_TIME_ZONE =>
        new TimeSingleValueAggFunction
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val d = argTypes(0).asInstanceOf[TimestampType]
        new TimestampSingleValueAggFunction(d)
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val ltzType = argTypes(0).asInstanceOf[LocalZonedTimestampType]
        new TimestampLtzSingleValueAggFunction(ltzType)
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

  private def createCumeDistAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    new CumeDistAggFunction
  }

  private def createRankAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    val argTypes = orderKeyIndexes.map(inputRowType.getChildren.get(_))
    new RankAggFunction(argTypes)
  }

  private def createDenseRankAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    val argTypes = orderKeyIndexes.map(inputRowType.getChildren.get(_))
    new DenseRankAggFunction(argTypes)
  }

  private def createPercentRankAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    val argTypes = orderKeyIndexes.map(inputRowType.getChildren.get(_))
    new PercentRankAggFunction(argTypes)
  }

  private def createNTILEAggFUnction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    new NTILEAggFunction
  }

  private def createFirstValueAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    val valueType = argTypes(0)
    if (aggCallNeedRetractions(index)) {
      valueType.getTypeRoot match {
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | VARCHAR | DECIMAL =>
          new FirstValueWithRetractAggFunction(valueType)
        case t =>
          throw new TableException(
            s"FIRST_VALUE with retract aggregate function does not " +
              s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      valueType.getTypeRoot match {
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | VARCHAR | DECIMAL =>
          new FirstValueAggFunction(valueType)
        case t =>
          throw new TableException(
            s"FIRST_VALUE aggregate function does not support " +
              s"type: ''$t''.\nPlease re-check the data type.")
      }
    }
  }

  private def createLastValueAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    val valueType = argTypes(0)
    if (aggCallNeedRetractions(index)) {
      valueType.getTypeRoot match {
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | VARCHAR | DECIMAL =>
          new LastValueWithRetractAggFunction(valueType)
        case t =>
          throw new TableException(
            s"LAST_VALUE with retract aggregate function does not " +
              s"support type: ''$t''.\nPlease re-check the data type.")
      }
    } else {
      valueType.getTypeRoot match {
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | VARCHAR | DECIMAL =>
          new LastValueAggFunction(valueType)
        case t =>
          throw new TableException(
            s"LAST_VALUE aggregate function does not support " +
              s"type: ''$t''.\nPlease re-check the data type.")
      }
    }
  }

  private def createListAggFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (aggCallNeedRetractions(index)) {
      new ListAggWithRetractAggFunction
    } else {
      new ListAggFunction(1)
    }
  }

  private def createListAggWsFunction(
      argTypes: Array[LogicalType],
      index: Int): UserDefinedFunction = {
    if (aggCallNeedRetractions(index)) {
      new ListAggWsWithRetractAggFunction
    } else {
      new ListAggFunction(2)
    }
  }

  private def createCollectAggFunction(argTypes: Array[LogicalType]): UserDefinedFunction = {
    new CollectAggFunction(argTypes(0))
  }
}
