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

package org.apache.flink.table.catalog

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.sql.ScalarSqlFunctions

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlReturnTypeInference, SqlTypeTransforms}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable
import org.apache.calcite.sql.validate.{SqlNameMatcher, SqlNameMatchers}

import _root_.java.util.{List => JList}
import java.util

import _root_.scala.collection.JavaConversions._

class BasicOperatorTable extends ReflectiveSqlOperatorTable {

  /**
    * List of supported SQL operators / functions.
    *
    * This list should be kept in sync with [[SqlStdOperatorTable]].
    */
  private val builtInSqlOperators: Seq[SqlOperator] = Seq(
    // SET OPERATORS
    SqlStdOperatorTable.UNION,
    SqlStdOperatorTable.UNION_ALL,
    SqlStdOperatorTable.EXCEPT,
    SqlStdOperatorTable.EXCEPT_ALL,
    SqlStdOperatorTable.INTERSECT,
    SqlStdOperatorTable.INTERSECT_ALL,
    // BINARY OPERATORS
    SqlStdOperatorTable.AND,
    SqlStdOperatorTable.AS,
    SqlStdOperatorTable.CONCAT,
    SqlStdOperatorTable.DIVIDE,
    SqlStdOperatorTable.DIVIDE_INTEGER,
    SqlStdOperatorTable.DOT,
    SqlStdOperatorTable.EQUALS,
    SqlStdOperatorTable.GREATER_THAN,
    SqlStdOperatorTable.IS_DISTINCT_FROM,
    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
    SqlStdOperatorTable.LESS_THAN,
    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
    SqlStdOperatorTable.MINUS,
    SqlStdOperatorTable.MULTIPLY,
    SqlStdOperatorTable.NOT_EQUALS,
    SqlStdOperatorTable.OR,
    SqlStdOperatorTable.PLUS,
    SqlStdOperatorTable.DATETIME_PLUS,
    // POSTFIX OPERATORS
    SqlStdOperatorTable.DESC,
    SqlStdOperatorTable.NULLS_FIRST,
    SqlStdOperatorTable.IS_NOT_NULL,
    SqlStdOperatorTable.IS_NULL,
    SqlStdOperatorTable.IS_NOT_TRUE,
    SqlStdOperatorTable.IS_TRUE,
    SqlStdOperatorTable.IS_NOT_FALSE,
    SqlStdOperatorTable.IS_FALSE,
    SqlStdOperatorTable.IS_NOT_UNKNOWN,
    SqlStdOperatorTable.IS_UNKNOWN,
    // PREFIX OPERATORS
    SqlStdOperatorTable.NOT,
    SqlStdOperatorTable.UNARY_MINUS,
    SqlStdOperatorTable.UNARY_PLUS,
    // GROUPING FUNCTIONS
    SqlStdOperatorTable.GROUP_ID,
    SqlStdOperatorTable.GROUPING,
    SqlStdOperatorTable.GROUPING_ID,
    // AGGREGATE OPERATORS
    SqlStdOperatorTable.SUM,
    SqlStdOperatorTable.SUM0,
    SqlStdOperatorTable.COUNT,
    SqlStdOperatorTable.COLLECT,
    SqlStdOperatorTable.MIN,
    SqlStdOperatorTable.MAX,
    SqlStdOperatorTable.AVG,
    SqlStdOperatorTable.STDDEV_POP,
    SqlStdOperatorTable.STDDEV_SAMP,
    SqlStdOperatorTable.VAR_POP,
    SqlStdOperatorTable.VAR_SAMP,
    // ARRAY OPERATORS
    SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
    SqlStdOperatorTable.ELEMENT,
    // MAP OPERATORS
    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
    // ARRAY MAP SHARED OPERATORS
    SqlStdOperatorTable.ITEM,
    SqlStdOperatorTable.CARDINALITY,
    // SPECIAL OPERATORS
    SqlStdOperatorTable.ROW,
    SqlStdOperatorTable.OVERLAPS,
    SqlStdOperatorTable.LITERAL_CHAIN,
    SqlStdOperatorTable.BETWEEN,
    SqlStdOperatorTable.SYMMETRIC_BETWEEN,
    SqlStdOperatorTable.NOT_BETWEEN,
    SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN,
    SqlStdOperatorTable.NOT_LIKE,
    SqlStdOperatorTable.LIKE,
    SqlStdOperatorTable.NOT_SIMILAR_TO,
    SqlStdOperatorTable.SIMILAR_TO,
    SqlStdOperatorTable.CASE,
    SqlStdOperatorTable.REINTERPRET,
    SqlStdOperatorTable.EXTRACT,
    SqlStdOperatorTable.IN,
    // FUNCTIONS
    SqlStdOperatorTable.SUBSTRING,
    SqlStdOperatorTable.OVERLAY,
    SqlStdOperatorTable.TRIM,
    SqlStdOperatorTable.POSITION,
    SqlStdOperatorTable.CHAR_LENGTH,
    SqlStdOperatorTable.CHARACTER_LENGTH,
    SqlStdOperatorTable.UPPER,
    SqlStdOperatorTable.LOWER,
    SqlStdOperatorTable.INITCAP,
    SqlStdOperatorTable.POWER,
    SqlStdOperatorTable.SQRT,
    SqlStdOperatorTable.MOD,
    SqlStdOperatorTable.LN,
    SqlStdOperatorTable.LOG10,
    ScalarSqlFunctions.LOG2,
    SqlStdOperatorTable.ABS,
    SqlStdOperatorTable.EXP,
    SqlStdOperatorTable.NULLIF,
    SqlStdOperatorTable.COALESCE,
    SqlStdOperatorTable.FLOOR,
    SqlStdOperatorTable.CEIL,
    SqlStdOperatorTable.LOCALTIME,
    SqlStdOperatorTable.LOCALTIMESTAMP,
    SqlStdOperatorTable.CURRENT_TIME,
    SqlStdOperatorTable.CURRENT_TIMESTAMP,
    SqlStdOperatorTable.CURRENT_DATE,
    ScalarSqlFunctions.DATE_FORMAT,
    SqlStdOperatorTable.CAST,
    SqlStdOperatorTable.SCALAR_QUERY,
    SqlStdOperatorTable.EXISTS,
    SqlStdOperatorTable.SIN,
    SqlStdOperatorTable.COS,
    SqlStdOperatorTable.TAN,
    ScalarSqlFunctions.TANH,
    SqlStdOperatorTable.COT,
    SqlStdOperatorTable.ASIN,
    SqlStdOperatorTable.ACOS,
    SqlStdOperatorTable.ATAN,
    SqlStdOperatorTable.ATAN2,
    ScalarSqlFunctions.COSH,
    SqlStdOperatorTable.DEGREES,
    SqlStdOperatorTable.RADIANS,
    SqlStdOperatorTable.SIGN,
    SqlStdOperatorTable.ROUND,
    SqlStdOperatorTable.PI,
    ScalarSqlFunctions.E,
    SqlStdOperatorTable.RAND,
    SqlStdOperatorTable.RAND_INTEGER,
    ScalarSqlFunctions.CONCAT,
    ScalarSqlFunctions.CONCAT_WS,
    SqlStdOperatorTable.REPLACE,
    ScalarSqlFunctions.BIN,
    ScalarSqlFunctions.HEX,
    ScalarSqlFunctions.LOG,
    ScalarSqlFunctions.LPAD,
    ScalarSqlFunctions.RPAD,
    ScalarSqlFunctions.MD5,
    ScalarSqlFunctions.SHA1,
    ScalarSqlFunctions.SINH,
    ScalarSqlFunctions.SHA224,
    ScalarSqlFunctions.SHA256,
    ScalarSqlFunctions.SHA384,
    ScalarSqlFunctions.SHA512,
    ScalarSqlFunctions.SHA2,
    ScalarSqlFunctions.REGEXP_EXTRACT,
    ScalarSqlFunctions.FROM_BASE64,
    ScalarSqlFunctions.TO_BASE64,
    ScalarSqlFunctions.UUID,
    ScalarSqlFunctions.LTRIM,
    ScalarSqlFunctions.RTRIM,
    ScalarSqlFunctions.REPEAT,
    ScalarSqlFunctions.REGEXP_REPLACE,
    SqlStdOperatorTable.TRUNCATE,

    // TIME FUNCTIONS
    SqlStdOperatorTable.YEAR,
    SqlStdOperatorTable.QUARTER,
    SqlStdOperatorTable.MONTH,
    SqlStdOperatorTable.WEEK,
    SqlStdOperatorTable.HOUR,
    SqlStdOperatorTable.MINUTE,
    SqlStdOperatorTable.SECOND,
    SqlStdOperatorTable.DAYOFYEAR,
    SqlStdOperatorTable.DAYOFMONTH,
    SqlStdOperatorTable.DAYOFWEEK,
    SqlStdOperatorTable.TIMESTAMP_ADD,
    SqlStdOperatorTable.TIMESTAMP_DIFF,

    // MATCH_RECOGNIZE
    SqlStdOperatorTable.FIRST,
    SqlStdOperatorTable.LAST,
    SqlStdOperatorTable.PREV,
    SqlStdOperatorTable.FINAL,
    SqlStdOperatorTable.RUNNING,
    BasicOperatorTable.MATCH_PROCTIME,
    BasicOperatorTable.MATCH_ROWTIME,

    // EXTENSIONS
    BasicOperatorTable.TUMBLE,
    BasicOperatorTable.HOP,
    BasicOperatorTable.SESSION,
    BasicOperatorTable.TUMBLE_START,
    BasicOperatorTable.TUMBLE_END,
    BasicOperatorTable.HOP_START,
    BasicOperatorTable.HOP_END,
    BasicOperatorTable.SESSION_START,
    BasicOperatorTable.SESSION_END,
    BasicOperatorTable.TUMBLE_PROCTIME,
    BasicOperatorTable.TUMBLE_ROWTIME,
    BasicOperatorTable.HOP_PROCTIME,
    BasicOperatorTable.HOP_ROWTIME,
    BasicOperatorTable.SESSION_PROCTIME,
    BasicOperatorTable.SESSION_ROWTIME
  )

  builtInSqlOperators.foreach(register)

  override def lookupOperatorOverloads(
      opName: SqlIdentifier,
      category: SqlFunctionCategory,
      syntax: SqlSyntax,
      operatorList: util.List[SqlOperator],
      nameMatcher: SqlNameMatcher): Unit = {
    // set caseSensitive=false to make sure the behavior is same with before.
    super.lookupOperatorOverloads(
      opName, category, syntax, operatorList, SqlNameMatchers.withCaseSensitive(false))
  }
}

object BasicOperatorTable {

  /**
    * We need custom group auxiliary functions in order to support nested windows.
    */

  val TUMBLE: SqlGroupedWindowFunction = new SqlGroupedWindowFunction(
    // The TUMBLE group function was hard code to $TUMBLE in CALCITE-3382.
    "$TUMBLE",
    SqlKind.TUMBLE,
    null,
    OperandTypes.or(OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME)) {
    override def getAuxiliaryFunctions: JList[SqlGroupedWindowFunction] =
      Seq(
        TUMBLE_START,
        TUMBLE_END,
        TUMBLE_ROWTIME,
        TUMBLE_PROCTIME)
  }
  val TUMBLE_START: SqlGroupedWindowFunction = TUMBLE.auxiliary(SqlKind.TUMBLE_START)
  val TUMBLE_END: SqlGroupedWindowFunction = TUMBLE.auxiliary(SqlKind.TUMBLE_END)
  val TUMBLE_ROWTIME: SqlGroupedWindowFunction =
    new SqlGroupedWindowFunction(
      "TUMBLE_ROWTIME",
      SqlKind.OTHER_FUNCTION,
      TUMBLE,
      // ensure that returned rowtime is always NOT_NULLABLE
      ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NOT_NULLABLE),
      null,
      TUMBLE.getOperandTypeChecker,
      SqlFunctionCategory.SYSTEM)
  val TUMBLE_PROCTIME: SqlGroupedWindowFunction =
    TUMBLE.auxiliary("TUMBLE_PROCTIME", SqlKind.OTHER_FUNCTION)

  val HOP: SqlGroupedWindowFunction = new SqlGroupedWindowFunction(
    SqlKind.HOP,
    null,
    OperandTypes.or(
      OperandTypes.DATETIME_INTERVAL_INTERVAL,
      OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME)) {
    override def getAuxiliaryFunctions: _root_.java.util.List[SqlGroupedWindowFunction] =
      Seq(
        HOP_START,
        HOP_END,
        HOP_ROWTIME,
        HOP_PROCTIME)
  }
  val HOP_START: SqlGroupedWindowFunction = HOP.auxiliary(SqlKind.HOP_START)
  val HOP_END: SqlGroupedWindowFunction = HOP.auxiliary(SqlKind.HOP_END)
  val HOP_ROWTIME: SqlGroupedWindowFunction =
    new SqlGroupedWindowFunction(
      "HOP_ROWTIME",
      SqlKind.OTHER_FUNCTION,
      HOP,
      // ensure that returned rowtime is always NOT_NULLABLE
      ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NOT_NULLABLE),
      null,
      HOP.getOperandTypeChecker,
      SqlFunctionCategory.SYSTEM)
  val HOP_PROCTIME: SqlGroupedWindowFunction = HOP.auxiliary("HOP_PROCTIME", SqlKind.OTHER_FUNCTION)

  val SESSION: SqlGroupedWindowFunction = new SqlGroupedWindowFunction(
    SqlKind.SESSION,
    null,
    OperandTypes.or(OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME)) {
    override def getAuxiliaryFunctions: _root_.java.util.List[SqlGroupedWindowFunction] =
      Seq(
        SESSION_START,
        SESSION_END,
        SESSION_ROWTIME,
        SESSION_PROCTIME)
  }
  val SESSION_START: SqlGroupedWindowFunction = SESSION.auxiliary(SqlKind.SESSION_START)
  val SESSION_END: SqlGroupedWindowFunction = SESSION.auxiliary(SqlKind.SESSION_END)
  val SESSION_ROWTIME: SqlGroupedWindowFunction =
    new SqlGroupedWindowFunction(
      "SESSION_ROWTIME",
      SqlKind.OTHER_FUNCTION,
      SESSION,
      // ensure that returned rowtime is always NOT_NULLABLE
      ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NOT_NULLABLE),
      null,
      SESSION.getOperandTypeChecker,
      SqlFunctionCategory.SYSTEM)
  val SESSION_PROCTIME: SqlGroupedWindowFunction =
    SESSION.auxiliary("SESSION_PROCTIME", SqlKind.OTHER_FUNCTION)

  private val RowTimeTypeInference = new TimeIndicatorReturnType(true)

  private val ProcTimeTypeInference = new TimeIndicatorReturnType(false)

  private class TimeIndicatorReturnType(isRowTime: Boolean) extends SqlReturnTypeInference {
    override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
      val flinkTypeFactory = opBinding.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      if (isRowTime) {
        flinkTypeFactory.createRowtimeIndicatorType()
      } else {
        flinkTypeFactory.createProctimeIndicatorType()
      }
    }
  }

  val MATCH_ROWTIME: SqlFunction =
    new SqlFunction(
      "MATCH_ROWTIME",
      SqlKind.OTHER_FUNCTION,
      RowTimeTypeInference,
      null,
      OperandTypes.NILADIC,
      SqlFunctionCategory.MATCH_RECOGNIZE
    ) {
      override def isDeterministic: Boolean = true
    }

  val MATCH_PROCTIME: SqlFunction =
    new SqlFunction(
      "MATCH_PROCTIME",
      SqlKind.OTHER_FUNCTION,
      ProcTimeTypeInference,
      null,
      OperandTypes.NILADIC,
      SqlFunctionCategory.MATCH_RECOGNIZE
    ) {
      override def isDeterministic: Boolean = false
    }
}
