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

package org.apache.flink.table.validate

import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.OperandTypes
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.util.{ChainedSqlOperatorTable, ListSqlOperatorTable, ReflectiveSqlOperatorTable}
import org.apache.flink.table.api._
import org.apache.flink.table.api.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.sql.{AggSqlFunctions, ScalarSqlFunctions}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{createTableSqlFunction, getResultTypeOfCTDFunction}
import org.apache.flink.table.functions.utils.{AggSqlFunction, ScalarSqlFunction, TableSqlFunction}
import org.apache.flink.table.runtime.functions.tablefunctions.{GenerateSeries, JsonTuple, MultiKeyValue, StringSplit}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.mutable
import _root_.scala.util.{Failure, Success, Try}

/**
  * A built in catalog for looking up (user-defined) functions, used during validation phases
  * of both Table API and SQL API.
  */
class BuiltInFunctionCatalog extends FunctionCatalog{

  private val functionBuilders = mutable.HashMap.empty[String, Class[_]]
  private val sqlFunctions = mutable.ListBuffer[SqlFunction]()

  override def registerFunction(name: String, builder: Class[_]): Unit =
    functionBuilders.put(name.toLowerCase, builder)

  override def registerSqlFunction(sqlFunction: SqlFunction): Unit = {
    sqlFunctions --= sqlFunctions.filter(_.getName == sqlFunction.getName)
    sqlFunctions += sqlFunction
  }

  override def getSqlOperatorTable: SqlOperatorTable = {
    ChainedSqlOperatorTable.of(
      new ListSqlOperatorTable(sqlFunctions),
      new BasicOperatorTable()
    )
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val funcClass = functionBuilders
      .getOrElse(name.toLowerCase, throw new ValidationException(s"Undefined function: $name"))

    // Instantiate a function using the provided `children`
    funcClass match {

      // user-defined scalar function call
      case sf if classOf[ScalarFunction].isAssignableFrom(sf) =>
        val scalarSqlFunction = sqlFunctions
          .find(f => f.getName.equalsIgnoreCase(name) && f.isInstanceOf[ScalarSqlFunction])
          .getOrElse(throw new ValidationException(s"Undefined scalar function: $name"))
          .asInstanceOf[ScalarSqlFunction]
        ScalarFunctionCall(scalarSqlFunction.getScalarFunction, children)

      // user-defined table function call
      case tf if classOf[TableFunction[_]].isAssignableFrom(tf) =>
        val tableSqlFunction = sqlFunctions
          .find(f => f.getName.equalsIgnoreCase(name) && f.isInstanceOf[TableSqlFunction])
          .getOrElse(throw new ValidationException(s"Undefined table function: $name"))
          .asInstanceOf[TableSqlFunction]
        val func = tableSqlFunction.getTableFunction
        TableFunctionCall(name, func, children, getResultTypeOfCTDFunction(
          func, children.toArray, () => tableSqlFunction.getImplicitResultType))

      // user-defined aggregate function call
      case af if classOf[AggregateFunction[_, _]].isAssignableFrom(af) =>
        val aggregateFunction = sqlFunctions
          .find(f => f.getName.equalsIgnoreCase(name) && f.isInstanceOf[AggSqlFunction])
          .getOrElse(throw new ValidationException(s"Undefined table function: $name"))
          .asInstanceOf[AggSqlFunction]
        val function = aggregateFunction.getFunction
        val externalResultType = aggregateFunction.externalResultType
        val externalAccType = aggregateFunction.externalAccType
        AggFunctionCall(
          function,
          externalResultType,
          externalAccType,
          children)

      // general expression call
      case expression if classOf[Expression].isAssignableFrom(expression) =>
        // try to find a constructor accepts `Seq[Expression]`
        Try(funcClass.getDeclaredConstructor(classOf[Seq[_]])) match {
          case Success(seqCtor) =>
            Try(seqCtor.newInstance(children).asInstanceOf[Expression]) match {
              case Success(expr) => expr
              case Failure(e) => throw new ValidationException(e.getMessage)
            }
          case Failure(_) =>
            Try(funcClass.getDeclaredConstructor(classOf[Expression], classOf[Seq[_]])) match {
              case Success(ctor) =>
                Try(ctor.newInstance(children.head, children.tail).asInstanceOf[Expression]) match {
                  case Success(expr) => expr
                  case Failure(e) => throw new ValidationException(e.getMessage)
                }
              case Failure(_) =>
                val childrenClass = Seq.fill(children.length)(classOf[Expression])
                // try to find a constructor matching the exact number of children
                Try(funcClass.getDeclaredConstructor(childrenClass: _*)) match {
                  case Success(ctor) =>
                    Try(ctor.newInstance(children: _*).asInstanceOf[Expression]) match {
                      case Success(expr) => expr
                      case Failure(exception) => throw new ValidationException(exception.getMessage)
                    }
                  case Failure(_) =>
                    throw new ValidationException(
                      s"Invalid number of arguments for function $funcClass")
                }
            }
        }
      case _ =>
        throw new ValidationException("Unsupported function.")
    }
  }

  override def dropFunction(name: String): Boolean =
    functionBuilders.remove(name.toLowerCase).isDefined

  override def clear(): Unit = functionBuilders.clear()
}

object BuiltInFunctionCatalog {

  val builtInFunctions: Map[String, Class[_]] = Map(

    // logic
    "and" -> classOf[And],
    "or" -> classOf[Or],
    "not" -> classOf[Not],
    "equals" -> classOf[EqualTo],
    "greaterThan" -> classOf[GreaterThan],
    "greaterThanOrEqual" -> classOf[GreaterThanOrEqual],
    "lessThan" -> classOf[LessThan],
    "lessThanOrEqual" -> classOf[LessThanOrEqual],
    "notEquals" -> classOf[NotEqualTo],
    "in" -> classOf[In],
    "isNull" -> classOf[IsNull],
    "isNotNull" -> classOf[IsNotNull],
    "isTrue" -> classOf[IsTrue],
    "isFalse" -> classOf[IsFalse],
    "isNotTrue" -> classOf[IsNotTrue],
    "isNotFalse" -> classOf[IsNotFalse],
    "if" -> classOf[If],
    "between" -> classOf[Between],
    "notBetween" -> classOf[NotBetween],

    // aggregate functions
    "avg" -> classOf[Avg],
    "count" -> classOf[Count],
    "max" -> classOf[Max],
    "min" -> classOf[Min],
    "sum" -> classOf[Sum],
    "sum0" -> classOf[Sum0],
    "incr_sum" -> classOf[IncrSum],
    "stddevPop" -> classOf[StddevPop],
    "stddevSamp" -> classOf[StddevSamp],
    "stddev" -> classOf[Stddev],
    "varPop" -> classOf[VarPop],
    "varSamp" -> classOf[VarSamp],
    "variance" -> classOf[Variance],
    "lead" -> classOf[Lead],
    "lag" -> classOf[Lag],
    "collect" -> classOf[Collect],
    "first_value" -> classOf[FirstValue],
    "last_value" -> classOf[LastValue],
    "concat_agg" -> classOf[ConcatAgg],
    "regexpReplace" -> classOf[RegexpReplace],

    // string functions
    "charLength" -> classOf[CharLength],
    "length" -> classOf[CharLength],
    "initCap" -> classOf[InitCap],
    "like" -> classOf[Like],
    "concat" -> classOf[Plus],
    "lower" -> classOf[Lower],
    "lowerCase" -> classOf[Lower],
    "similar" -> classOf[Similar],
    "regexpExtract" -> classOf[RegexpExtract],
    "substring" -> classOf[Substring],
    "replace" -> classOf[Replace],
    "left" -> classOf[Left],
    "right" -> classOf[Right],
    "trim" -> classOf[Trim],
    "ltrim" -> classOf[Ltrim],
    "rtrim" -> classOf[Rtrim],
    "repeat" -> classOf[Repeat],
    "upper" -> classOf[Upper],
    "upperCase" -> classOf[Upper],
    "position" -> classOf[Position],
    "overlay" -> classOf[Overlay],
    "concat" -> classOf[Concat],
    "concat_ws" -> classOf[ConcatWs],
    "lpad" -> classOf[Lpad],
    "rpad" -> classOf[Rpad],
    "locate" -> classOf[Locate],
    "ascii" -> classOf[Ascii],
    "encode" -> classOf[Encode],
    "decode" -> classOf[Decode],
    "instr" -> classOf[Instr],
    "uuid" -> classOf[UUID],
    "fromBase64" -> classOf[FromBase64],
    "toBase64" -> classOf[ToBase64],

    // math functions
    "plus" -> classOf[Plus],
    "minus" -> classOf[Minus],
    "divide" -> classOf[Div],
    "times" -> classOf[Mul],
    "abs" -> classOf[Abs],
    "ceil" -> classOf[Ceil],
    "exp" -> classOf[Exp],
    "floor" -> classOf[Floor],
    "log10" -> classOf[Log10],
    "log2" -> classOf[Log2],
    "ln" -> classOf[Ln],
    "log" -> classOf[Log],
    "power" -> classOf[Power],
    "mod" -> classOf[Mod],
    "sqrt" -> classOf[Sqrt],
    "minusPrefix" -> classOf[UnaryMinus],
    "sin" -> classOf[Sin],
    "cos" -> classOf[Cos],
    "tan" -> classOf[Tan],
    "cot" -> classOf[Cot],
    "asin" -> classOf[Asin],
    "acos" -> classOf[Acos],
    "atan" -> classOf[Atan],
    "atan2" -> classOf[Atan2],
    "sinh" -> classOf[Sinh],
    "cosh" -> classOf[Cosh],
    "tanh" -> classOf[Tanh],
    "degrees" -> classOf[Degrees],
    "radians" -> classOf[Radians],
    "sign" -> classOf[Sign],
    "round" -> classOf[Round],
    "pi" -> classOf[Pi],
    "e" -> classOf[E],
    "rand" -> classOf[Rand],
    "randInteger" -> classOf[RandInteger],
    "bin" -> classOf[Bin],
    "hex" -> classOf[Hex],

    // temporal functions
    "extract" -> classOf[Extract],
    "currentDate" -> classOf[CurrentDate],
    "currentTime" -> classOf[CurrentTime],
    "currentTimestamp" -> classOf[CurrentTimestamp],
    "localTime" -> classOf[LocalTime],
    "localTimestamp" -> classOf[LocalTimestamp],
    "quarter" -> classOf[Quarter],
    "temporalOverlaps" -> classOf[TemporalOverlaps],
    "dateTimePlus" -> classOf[Plus],
    "dateFormat" -> classOf[DateFormat],
    "timestampDiff" -> classOf[TimestampDiff],

    // row
    "row" -> classOf[RowConstructor],

    // array
    "array" -> classOf[ArrayConstructor],
    "element" -> classOf[ArrayElement],

    // map
    "map" -> classOf[MapConstructor],

    // item
    "at" -> classOf[ItemAt],

    // cardinality
    "cardinality" -> classOf[Cardinality],

    // window properties
    "start" -> classOf[WindowStart],
    "end" -> classOf[WindowEnd],

    // ordering
    "asc" -> classOf[Asc],
    "desc" -> classOf[Desc],
    "nullsFirst" -> classOf[NullsFirst],
    "nullsLast" -> classOf[NullsLast],

    // crypto hash
    "md5" -> classOf[Md5],
    "sha1" -> classOf[Sha1],
    "sha224" -> classOf[Sha224],
    "sha256" -> classOf[Sha256],
    "sha384" -> classOf[Sha384],
    "sha512" -> classOf[Sha512],
    "sha2" -> classOf[Sha2],

    "proctime" -> classOf[Proctime]
  )

  val buildInTableFunctions: Map[String, TableFunction[_]] = Map(
    "string_split" -> new StringSplit,
    "json_tuple" -> new JsonTuple,
    "generate_series" -> new GenerateSeries,
    "multi_keyvalue" -> new MultiKeyValue
  )

  /**
    * Create a new function catalog with built-in functions.
    */
  def withBuiltIns(): FunctionCatalog = {
    val catalog = new BuiltInFunctionCatalog
    builtInFunctions.foreach { case (name, clazz) => catalog.registerFunction(name, clazz) }
    buildInTableFunctions.foreach { case (name, tableFunction) =>
      List(name, name.toUpperCase).foreach { regName =>
        catalog.registerSqlFunction(
          createTableSqlFunction(
            regName,
            regName,
            tableFunction,
            // TODO correct args for builtInTableFunctions.
            tableFunction.getResultType(Array[AnyRef](), Array[Class[_]]()),
            // TODO different with typeFactory in TableEnvironment may cause some problems.
            new FlinkTypeFactory(new FlinkTypeSystem())))
      }
    }
    catalog
  }
}

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
    ScalarSqlFunctions.DIVIDE,
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
    SqlStdOperatorTable.NULLS_LAST,
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
    AggSqlFunctions.INCR_SUM,
    SqlStdOperatorTable.COUNT,
    SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
    AggSqlFunctions.CARDINALITY_COUNT,
    SqlStdOperatorTable.COLLECT,
    SqlStdOperatorTable.MULTISET_VALUE,
    SqlStdOperatorTable.MIN,
    SqlStdOperatorTable.MAX,
    AggSqlFunctions.MAX2ND,
    SqlStdOperatorTable.AVG,
    SqlStdOperatorTable.STDDEV,
    SqlStdOperatorTable.STDDEV_POP,
    SqlStdOperatorTable.STDDEV_SAMP,
    SqlStdOperatorTable.VARIANCE,
    SqlStdOperatorTable.VAR_POP,
    SqlStdOperatorTable.VAR_SAMP,
    AggSqlFunctions.FIRST_VALUE,
    AggSqlFunctions.LAST_VALUE,
    AggSqlFunctions.CONCAT_AGG,
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
    SqlStdOperatorTable.EXTRACT,
    SqlStdOperatorTable.QUARTER,
    SqlStdOperatorTable.SCALAR_QUERY,
    SqlStdOperatorTable.EXISTS,
    SqlStdOperatorTable.SIN,
    SqlStdOperatorTable.COS,
    SqlStdOperatorTable.TAN,
    SqlStdOperatorTable.COT,
    SqlStdOperatorTable.ASIN,
    SqlStdOperatorTable.ACOS,
    SqlStdOperatorTable.ATAN,
    SqlStdOperatorTable.ATAN2,
    ScalarSqlFunctions.SINH,
    ScalarSqlFunctions.COSH,
    ScalarSqlFunctions.TANH,
    SqlStdOperatorTable.DEGREES,
    SqlStdOperatorTable.RADIANS,
    SqlStdOperatorTable.SIGN,
    ScalarSqlFunctions.ROUND,
    ScalarSqlFunctions.PI,
    SqlStdOperatorTable.PI,
    ScalarSqlFunctions.E,
    SqlStdOperatorTable.RAND,
    ScalarSqlFunctions.HEX,
    SqlStdOperatorTable.RAND_INTEGER,
    ScalarSqlFunctions.CONCAT,
    ScalarSqlFunctions.CONCAT_WS,
    SqlStdOperatorTable.TIMESTAMP_ADD,
    SqlStdOperatorTable.TIMESTAMP_DIFF,
    ScalarSqlFunctions.LOG,

    // MATCH_RECOGNIZE
    SqlStdOperatorTable.FIRST,
    SqlStdOperatorTable.LAST,
    SqlStdOperatorTable.PREV,
    SqlStdOperatorTable.NEXT,
    SqlStdOperatorTable.CLASSIFIER,
    SqlStdOperatorTable.FINAL,
    SqlStdOperatorTable.RUNNING,
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
    BasicOperatorTable.SESSION_ROWTIME,
    SqlStdOperatorTable.RANK,
    SqlStdOperatorTable.DENSE_RANK,
    SqlStdOperatorTable.ROW_NUMBER,
    SqlStdOperatorTable.LEAD,
    SqlStdOperatorTable.LAG,

    // BLINK-build-in functions
    ScalarSqlFunctions.LENGTH,
    ScalarSqlFunctions.JSON_VALUE,
    ScalarSqlFunctions.STR_TO_MAP,
    ScalarSqlFunctions.LOG2,
    ScalarSqlFunctions.IS_DECIMAL,
    ScalarSqlFunctions.IS_DIGIT,
    ScalarSqlFunctions.IS_ALPHA,
    ScalarSqlFunctions.IF,
    ScalarSqlFunctions.CHR,
    ScalarSqlFunctions.LPAD,
    ScalarSqlFunctions.RPAD,
    ScalarSqlFunctions.REPEAT,
    ScalarSqlFunctions.REVERSE,
    ScalarSqlFunctions.REPLACE,
    ScalarSqlFunctions.SPLIT_INDEX,
    ScalarSqlFunctions.REGEXP_REPLACE,
    ScalarSqlFunctions.REGEXP_EXTRACT,
    ScalarSqlFunctions.KEYVALUE,
    ScalarSqlFunctions.HASH_CODE,
    ScalarSqlFunctions.MD5,
    ScalarSqlFunctions.SHA1,
    ScalarSqlFunctions.SHA224,
    ScalarSqlFunctions.SHA256,
    ScalarSqlFunctions.SHA384,
    ScalarSqlFunctions.SHA512,
    ScalarSqlFunctions.SHA2,
    ScalarSqlFunctions.REGEXP,
    ScalarSqlFunctions.PARSE_URL,
    ScalarSqlFunctions.PRINT,
    ScalarSqlFunctions.SUBSTRING,
    ScalarSqlFunctions.SUBSTR,
    ScalarSqlFunctions.LEFT,
    ScalarSqlFunctions.RIGHT,
    ScalarSqlFunctions.LOCATE,
    ScalarSqlFunctions.NOW,
    ScalarSqlFunctions.UNIX_TIMESTAMP,
    ScalarSqlFunctions.FROM_UNIXTIME,
    ScalarSqlFunctions.DATEDIFF,
    ScalarSqlFunctions.DATE_SUB,
    ScalarSqlFunctions.DATE_ADD,
    ScalarSqlFunctions.BIN,
    ScalarSqlFunctions.BITAND,
    ScalarSqlFunctions.BITNOT,
    ScalarSqlFunctions.BITOR,
    ScalarSqlFunctions.BITXOR,
    ScalarSqlFunctions.DIV,
    ScalarSqlFunctions.DIV_INT,
    ScalarSqlFunctions.TO_BASE64,
    ScalarSqlFunctions.FROM_BASE64,
    ScalarSqlFunctions.UUID,
    ScalarSqlFunctions.TO_TIMESTAMP,
    ScalarSqlFunctions.TO_TIMESTAMP_TZ,
    ScalarSqlFunctions.DATE_FORMAT_TZ,
    ScalarSqlFunctions.CONVERT_TZ,
    ScalarSqlFunctions.FROM_TIMESTAMP,
    ScalarSqlFunctions.TO_DATE,
    ScalarSqlFunctions.PROCTIME,
    ScalarSqlFunctions.ASCII,
    ScalarSqlFunctions.ENCODE,
    ScalarSqlFunctions.DECODE,
    ScalarSqlFunctions.INSTR,
    ScalarSqlFunctions.LTRIM,
    ScalarSqlFunctions.RTRIM
  )

  builtInSqlOperators.foreach(register)
}

object BasicOperatorTable {

   /**
     * We need custom group auxiliary functions in order to support nested windows.
     */
   val TUMBLE: SqlGroupedWindowFunction = new SqlGroupedWindowFunction(
     SqlKind.TUMBLE,
     null,
     OperandTypes.or(OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME)) {
     override def getAuxiliaryFunctions: _root_.java.util.List[SqlGroupedWindowFunction] =
       Seq(
         TUMBLE_START,
         TUMBLE_END,
         TUMBLE_ROWTIME,
         TUMBLE_PROCTIME)
   }
   val TUMBLE_START: SqlGroupedWindowFunction = TUMBLE.auxiliary(SqlKind.TUMBLE_START)
   val TUMBLE_END: SqlGroupedWindowFunction = TUMBLE.auxiliary(SqlKind.TUMBLE_END)
   val TUMBLE_ROWTIME: SqlGroupedWindowFunction =
     TUMBLE.auxiliary("TUMBLE_ROWTIME", SqlKind.OTHER_FUNCTION)
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
   val HOP_ROWTIME: SqlGroupedWindowFunction = HOP.auxiliary("HOP_ROWTIME", SqlKind.OTHER_FUNCTION)
   val HOP_PROCTIME: SqlGroupedWindowFunction =
     HOP.auxiliary("HOP_PROCTIME", SqlKind.OTHER_FUNCTION)

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
     SESSION.auxiliary("SESSION_ROWTIME", SqlKind.OTHER_FUNCTION)
   val SESSION_PROCTIME: SqlGroupedWindowFunction =
     SESSION.auxiliary("SESSION_PROCTIME", SqlKind.OTHER_FUNCTION)

 }
