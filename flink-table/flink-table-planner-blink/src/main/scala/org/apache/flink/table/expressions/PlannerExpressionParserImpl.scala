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
package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api._
import org.apache.flink.table.delegation.PlannerExpressionParser
import ApiExpressionUtils._
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

import _root_.java.math.{BigDecimal => JBigDecimal}
import _root_.java.util.{List => JList}

import _root_.scala.collection.JavaConversions._
import _root_.scala.language.implicitConversions
import _root_.scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}

/**
  * The implementation of a [[PlannerExpressionParser]] which parsers expressions inside a String.
  *
  * <p><strong>WARNING</strong>: please keep this class in sync with PlannerExpressionParserImpl
  * variant in flink-table-planner module.
  */
class PlannerExpressionParserImpl extends PlannerExpressionParser {

  def parseExpression(exprString: String): Expression = {
    PlannerExpressionParserImpl.parseExpression(exprString)
  }

  override def parseExpressionList(expression: String): JList[Expression] = {
    PlannerExpressionParserImpl.parseExpressionList(expression)
  }
}

/**
 * Parser for expressions inside a String. This parses exactly the same expressions that
 * would be accepted by the Scala Expression DSL.
 *
 * See ImplicitExpressionConversions and ImplicitExpressionOperations for the constructs
 * available in the Scala Expression DSL. This parser must be kept in sync with the Scala DSL
 * lazy valined in the above files.
 */
object PlannerExpressionParserImpl extends JavaTokenParsers
  with PackratParsers
  with PlannerExpressionParser {

  case class Keyword(key: String)

  // Convert the keyword into an case insensitive Parser
  // The pattern ensures that the keyword is not matched as a prefix, i.e.,
  //   the keyword is not followed by a Java identifier character.
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E(?![_$\p{javaJavaIdentifierPart}])""").r
  }

  // Keyword
  lazy val AS: Keyword = Keyword("as")
  lazy val CAST: Keyword = Keyword("cast")
  lazy val ASC: Keyword = Keyword("asc")
  lazy val DESC: Keyword = Keyword("desc")
  lazy val NULL: Keyword = Keyword("Null")
  lazy val NULL_OF: Keyword = Keyword("nullOf")
  lazy val IF: Keyword = Keyword("?")
  lazy val TO_DATE: Keyword = Keyword("toDate")
  lazy val TO_TIME: Keyword = Keyword("toTime")
  lazy val TO_TIMESTAMP: Keyword = Keyword("toTimestamp")
  lazy val TRIM: Keyword = Keyword("trim")
  lazy val EXTRACT: Keyword = Keyword("extract")
  lazy val TIMESTAMP_DIFF: Keyword = Keyword("timestampDiff")
  lazy val FLOOR: Keyword = Keyword("floor")
  lazy val CEIL: Keyword = Keyword("ceil")
  lazy val LOG: Keyword = Keyword("log")
  lazy val YEARS: Keyword = Keyword("years")
  lazy val YEAR: Keyword = Keyword("year")
  lazy val QUARTERS: Keyword = Keyword("quarters")
  lazy val QUARTER: Keyword = Keyword("quarter")
  lazy val MONTHS: Keyword = Keyword("months")
  lazy val MONTH: Keyword = Keyword("month")
  lazy val WEEKS: Keyword = Keyword("weeks")
  lazy val WEEK: Keyword = Keyword("week")
  lazy val DAYS: Keyword = Keyword("days")
  lazy val DAY: Keyword = Keyword("day")
  lazy val HOURS: Keyword = Keyword("hours")
  lazy val HOUR: Keyword = Keyword("hour")
  lazy val MINUTES: Keyword = Keyword("minutes")
  lazy val MINUTE: Keyword = Keyword("minute")
  lazy val SECONDS: Keyword = Keyword("seconds")
  lazy val SECOND: Keyword = Keyword("second")
  lazy val MILLIS: Keyword = Keyword("millis")
  lazy val MILLI: Keyword = Keyword("milli")
  lazy val ROWS: Keyword = Keyword("rows")
  lazy val STAR: Keyword = Keyword("*")
  lazy val GET: Keyword = Keyword("get")
  lazy val FLATTEN: Keyword = Keyword("flatten")
  lazy val OVER: Keyword = Keyword("over")
  lazy val DISTINCT: Keyword = Keyword("distinct")
  lazy val CURRENT_ROW: Keyword = Keyword("current_row")
  lazy val CURRENT_RANGE: Keyword = Keyword("current_range")
  lazy val UNBOUNDED_ROW: Keyword = Keyword("unbounded_row")
  lazy val UNBOUNDED_RANGE: Keyword = Keyword("unbounded_range")
  lazy val ROWTIME: Keyword = Keyword("rowtime")
  lazy val PROCTIME: Keyword = Keyword("proctime")
  lazy val TRUE: Keyword = Keyword("true")
  lazy val FALSE: Keyword = Keyword("false")
  lazy val PRIMITIVE_ARRAY: Keyword = Keyword("PRIMITIVE_ARRAY")
  lazy val OBJECT_ARRAY: Keyword = Keyword("OBJECT_ARRAY")
  lazy val MAP: Keyword = Keyword("MAP")
  lazy val BYTE: Keyword = Keyword("BYTE")
  lazy val SHORT: Keyword = Keyword("SHORT")
  lazy val INTERVAL_MONTHS: Keyword = Keyword("INTERVAL_MONTHS")
  lazy val INTERVAL_MILLIS: Keyword = Keyword("INTERVAL_MILLIS")
  lazy val INT: Keyword = Keyword("INT")
  lazy val LONG: Keyword = Keyword("LONG")
  lazy val FLOAT: Keyword = Keyword("FLOAT")
  lazy val DOUBLE: Keyword = Keyword("DOUBLE")
  lazy val BOOLEAN: Keyword = Keyword("BOOLEAN")
  lazy val STRING: Keyword = Keyword("STRING")
  lazy val SQL_DATE: Keyword = Keyword("SQL_DATE")
  lazy val SQL_TIMESTAMP: Keyword = Keyword("SQL_TIMESTAMP")
  lazy val SQL_TIME: Keyword = Keyword("SQL_TIME")
  lazy val DECIMAL: Keyword = Keyword("DECIMAL")
  lazy val TRIM_MODE_LEADING: Keyword = Keyword("LEADING")
  lazy val TRIM_MODE_TRAILING: Keyword = Keyword("TRAILING")
  lazy val TRIM_MODE_BOTH: Keyword = Keyword("BOTH")
  lazy val TO: Keyword = Keyword("TO")

  def functionIdent: PlannerExpressionParserImpl.Parser[String] = super.ident

  // symbols

  lazy val timeIntervalUnit: PackratParser[Expression] = TimeIntervalUnit.values map {
    unit: TimeIntervalUnit => literal(unit.toString) ^^^ valueLiteral(unit)
  } reduceLeft(_ | _)

  lazy val timePointUnit: PackratParser[Expression] = TimePointUnit.values map {
    unit: TimePointUnit => literal(unit.toString) ^^^ valueLiteral(unit)
  } reduceLeft(_ | _)

  lazy val currentRange: PackratParser[Expression] = CURRENT_RANGE ^^ {
    _ => unresolvedCall(BuiltInFunctionDefinitions.CURRENT_RANGE)
  }

  lazy val currentRow: PackratParser[Expression] = CURRENT_ROW ^^ {
    _ => unresolvedCall(BuiltInFunctionDefinitions.CURRENT_ROW)
  }

  lazy val unboundedRange: PackratParser[Expression] = UNBOUNDED_RANGE ^^ {
    _ => unresolvedCall(BuiltInFunctionDefinitions.UNBOUNDED_RANGE)
  }

  lazy val unboundedRow: PackratParser[Expression] = UNBOUNDED_ROW ^^ {
    _ => unresolvedCall(BuiltInFunctionDefinitions.UNBOUNDED_ROW)
  }

  lazy val overConstant: PackratParser[Expression] =
    currentRange | currentRow | unboundedRange | unboundedRow

  lazy val trimMode: PackratParser[String] =
    TRIM_MODE_LEADING | TRIM_MODE_TRAILING | TRIM_MODE_BOTH

  // data types

  lazy val dataType: PackratParser[TypeInformation[_]] =
    PRIMITIVE_ARRAY ~ "(" ~> dataType <~ ")" ^^ { ct => Types.PRIMITIVE_ARRAY(ct) } |
    OBJECT_ARRAY ~ "(" ~> dataType <~ ")" ^^ { ct => Types.OBJECT_ARRAY(ct) } |
    MAP ~ "(" ~> dataType ~ "," ~ dataType <~ ")" ^^ { mt => Types.MAP(mt._1._1, mt._2)} |
    BYTE ^^ { e => Types.BYTE } |
    SHORT ^^ { e => Types.SHORT } |
    INTERVAL_MONTHS ^^ { e => Types.INTERVAL_MONTHS } |
    INTERVAL_MILLIS ^^ { e => Types.INTERVAL_MILLIS } |
    INT ^^ { e => Types.INT } |
    LONG ^^ { e => Types.LONG } |
    FLOAT ^^ { e => Types.FLOAT } |
    DOUBLE ^^ { e => Types.DOUBLE } |
    BOOLEAN ^^ { { e => Types.BOOLEAN } } |
    STRING ^^ { e => Types.STRING } |
    SQL_DATE ^^ { e => Types.SQL_DATE } |
    SQL_TIMESTAMP ^^ { e => Types.SQL_TIMESTAMP } |
    SQL_TIME ^^ { e => Types.SQL_TIME } |
    DECIMAL ^^ { e => Types.DECIMAL }

  // literals

  // same as floatingPointNumber but we do not allow trailing dot "12.d" or "2."
  lazy val floatingPointNumberFlink: Parser[String] =
    """-?(\d+(\.\d+)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r

  lazy val numberLiteral: PackratParser[Expression] =
    (wholeNumber <~ ("l" | "L")) ^^ { n => valueLiteral(n.toLong) } |
      (decimalNumber <~ ("p" | "P")) ^^ { n => valueLiteral(new JBigDecimal(n)) } |
      (floatingPointNumberFlink | decimalNumber) ^^ {
        n =>
          if (n.matches("""-?\d+""")) {
            valueLiteral(n.toInt)
          } else if (n.endsWith("f") || n.endsWith("F")) {
            valueLiteral(n.toFloat)
          } else {
            valueLiteral(n.toDouble)
          }
      }

  // string with single quotes such as 'It''s me.'
  lazy val singleQuoteStringLiteral: Parser[Expression] = "'(?:''|[^'])*'".r ^^ {
    str =>
      val escaped = str.substring(1, str.length - 1).replace("''", "'")
      valueLiteral(escaped)
  }

  // string with double quotes such as "I ""like"" dogs."
  lazy val doubleQuoteStringLiteral: PackratParser[Expression] = "\"(?:\"\"|[^\"])*\"".r ^^ {
    str =>
      val escaped = str.substring(1, str.length - 1).replace("\"\"", "\"")
      valueLiteral(escaped)
  }

  lazy val boolLiteral: PackratParser[Expression] = (TRUE | FALSE) ^^ {
    str => valueLiteral(str.toBoolean)
  }

  lazy val nullLiteral: PackratParser[Expression] = (NULL | NULL_OF) ~ "(" ~> dataType <~ ")" ^^ {
    dt => valueLiteral(null, fromLegacyInfoToDataType(dt))
  }

  lazy val literalExpr: PackratParser[Expression] =
    numberLiteral | doubleQuoteStringLiteral | singleQuoteStringLiteral | boolLiteral

  lazy val fieldReference: PackratParser[UnresolvedReferenceExpression] = (STAR | ident) ^^ {
    sym => unresolvedRef(sym)
  }

  lazy val atom: PackratParser[Expression] =
    ( "(" ~> expression <~ ")" ) | (fieldReference ||| literalExpr)

  lazy val over: PackratParser[Expression] = composite ~ OVER ~ fieldReference ^^ {
    case agg ~ _ ~ windowRef =>
      unresolvedCall(BuiltInFunctionDefinitions.OVER, agg, windowRef)
  }

  // suffix operators

  lazy val suffixAsc : PackratParser[Expression] = composite <~ "." ~ ASC ~ opt("()") ^^ { e =>
      unresolvedCall(BuiltInFunctionDefinitions.ORDER_ASC, e)
  }

  lazy val suffixDesc : PackratParser[Expression] = composite <~ "." ~ DESC ~ opt("()") ^^ { e =>
    unresolvedCall(BuiltInFunctionDefinitions.ORDER_DESC, e)
  }

  lazy val suffixCast: PackratParser[Expression] =
    composite ~ "." ~ CAST ~ "(" ~ dataType ~ ")" ^^ {
      case e ~ _ ~ _ ~ _ ~ dt ~ _ =>
        unresolvedCall(
          BuiltInFunctionDefinitions.CAST,
          e,
          typeLiteral(fromLegacyInfoToDataType(dt)))
    }

  lazy val suffixTrim: PackratParser[Expression] =
    composite ~ "." ~ TRIM ~ "(" ~ trimMode ~
        "," ~ expression ~ ")" ^^ {
      case operand ~ _ ~ _ ~ _ ~ mode ~ _ ~ trimCharacter ~ _ =>
        unresolvedCall(
          BuiltInFunctionDefinitions.TRIM,
          valueLiteral(mode == TRIM_MODE_LEADING.key || mode == TRIM_MODE_BOTH.key),
          valueLiteral(mode == TRIM_MODE_TRAILING.key || mode == TRIM_MODE_BOTH.key),
          trimCharacter,
          operand)
    }

  lazy val suffixTrimWithoutArgs: PackratParser[Expression] =
    composite <~ "." ~ TRIM ~ opt("()") ^^ {
      e =>
        unresolvedCall(
          BuiltInFunctionDefinitions.TRIM,
          valueLiteral(true),
          valueLiteral(true),
          valueLiteral(" "),
          e)
    }

  lazy val suffixIf: PackratParser[Expression] =
    composite ~ "." ~ IF ~ "(" ~ expression ~ "," ~ expression ~ ")" ^^ {
      case condition ~ _ ~ _ ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.IF, condition, ifTrue, ifFalse)
    }

  lazy val suffixExtract: PackratParser[Expression] =
    composite ~ "." ~ EXTRACT ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
      case operand ~ _  ~ _ ~ _ ~ unit ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.EXTRACT, unit, operand)
    }

  lazy val suffixFloor: PackratParser[Expression] =
    composite ~ "." ~ FLOOR ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
      case operand ~ _  ~ _ ~ _ ~ unit ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.FLOOR, operand, unit)
    }

  lazy val suffixCeil: PackratParser[Expression] =
    composite ~ "." ~ CEIL ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
      case operand ~ _  ~ _ ~ _ ~ unit ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.CEIL, operand, unit)
    }

  // required because op.log(base) changes order of a parameters
  lazy val suffixLog: PackratParser[Expression] =
    composite ~ "." ~ LOG ~ "(" ~ expression ~ ")" ^^ {
      case operand ~ _ ~ _ ~ _ ~ base ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.LOG, base, operand)
    }

  lazy val suffixFunctionCall: PackratParser[Expression] =
    composite ~ "." ~ functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
    case operand ~ _ ~ name ~ _ ~ args ~ _ =>
      lookupCall(name, operand :: args: _*)
  }

  lazy val suffixFunctionCallOneArg: PackratParser[Expression] =
    composite ~ "." ~ functionIdent ^^ {
      case operand ~ _ ~ name =>
        lookupCall(name, operand)
    }

  lazy val suffixToDate: PackratParser[Expression] =
    composite <~ "." ~ TO_DATE ~ opt("()") ^^ { e =>
      unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        e,
        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.DATE)))
    }

  lazy val suffixToTimestamp: PackratParser[Expression] =
    composite <~ "." ~ TO_TIMESTAMP ~ opt("()") ^^ { e =>
      unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        e,
        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIMESTAMP)))
    }

  lazy val suffixToTime: PackratParser[Expression] =
    composite <~ "." ~ TO_TIME ~ opt("()") ^^ { e =>
      unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        e,
        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIME)))
    }

  lazy val suffixTimeInterval : PackratParser[Expression] =
    composite ~ "." ~ (YEARS | QUARTERS | MONTHS | WEEKS | DAYS |  HOURS | MINUTES |
      SECONDS | MILLIS | YEAR | QUARTER | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | MILLI) ^^ {

    case expr ~ _ ~ (YEARS.key | YEAR.key) => toMonthInterval(expr, 12)

    case expr ~ _ ~ (QUARTERS.key | QUARTER.key) => toMonthInterval(expr, 3)

    case expr ~ _ ~ (MONTHS.key | MONTH.key) => toMonthInterval(expr, 1)

    case expr ~ _ ~ (WEEKS.key | WEEK.key) => toMilliInterval(expr, 7 * MILLIS_PER_DAY)

    case expr ~ _ ~ (DAYS.key | DAY.key) => toMilliInterval(expr, MILLIS_PER_DAY)

    case expr ~ _ ~ (HOURS.key | HOUR.key) => toMilliInterval(expr, MILLIS_PER_HOUR)

    case expr ~ _ ~ (MINUTES.key | MINUTE.key) => toMilliInterval(expr, MILLIS_PER_MINUTE)

    case expr ~ _ ~ (SECONDS.key | SECOND.key) => toMilliInterval(expr, MILLIS_PER_SECOND)

    case expr ~ _ ~ (MILLIS.key | MILLI.key)=> toMilliInterval(expr, 1)
  }

  lazy val suffixRowInterval : PackratParser[Expression] =
    composite <~ "." ~ ROWS ^^ { e => toRowInterval(e) }

  lazy val suffixGet: PackratParser[Expression] =
    composite ~ "." ~ GET ~ "(" ~ literalExpr ~ ")" ^^ {
      case e ~ _ ~ _ ~ _ ~ index ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.GET, e, index)
    }

  lazy val suffixFlattening: PackratParser[Expression] =
    composite <~ "." ~ FLATTEN ~ opt("()") ^^ { e =>
      unresolvedCall(BuiltInFunctionDefinitions.FLATTEN, e)
    }

  lazy val suffixDistinct: PackratParser[Expression] =
    composite <~ "." ~ DISTINCT ~ opt("()") ^^ { e =>
      unresolvedCall(BuiltInFunctionDefinitions.DISTINCT, e)
    }

  lazy val suffixAs: PackratParser[Expression] =
    composite ~ "." ~ AS ~ "(" ~ rep1sep(fieldReference, ",") ~ ")" ^^ {
      case e ~ _ ~ _ ~ _ ~ names ~ _ =>
        unresolvedCall(
          BuiltInFunctionDefinitions.AS,
          e :: names.map(n => valueLiteral(n.getName)): _*)
  }

  lazy val suffixed: PackratParser[Expression] =
    // expressions that need to be resolved early
    suffixFlattening |
    // expressions that need special expression conversion
    suffixAs | suffixTimeInterval | suffixRowInterval | suffixToTimestamp | suffixToTime |
    suffixToDate |
    // expression for log
    suffixLog |
    // expression for ordering
    suffixAsc | suffixDesc |
    // expressions that take enumerations
    suffixCast | suffixTrim | suffixTrimWithoutArgs | suffixExtract | suffixFloor | suffixCeil |
    // expressions that take literals
    suffixGet |
    // expression with special identifier
    suffixIf |
    // expression with distinct suffix modifier
    suffixDistinct |
    // function call must always be at the end
    suffixFunctionCall | suffixFunctionCallOneArg |
    // rowtime or proctime
    timeIndicator

  // prefix operators

  lazy val prefixCast: PackratParser[Expression] =
    CAST ~ "(" ~ expression ~ "," ~ dataType ~ ")" ^^ {
      case _ ~ _ ~ e ~ _ ~ dt ~ _ =>
        unresolvedCall(
          BuiltInFunctionDefinitions.CAST,
          e,
          typeLiteral(fromLegacyInfoToDataType(dt)))
    }

  lazy val prefixIf: PackratParser[Expression] =
    IF ~ "(" ~ expression ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
      case _ ~ _ ~ condition ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.IF, condition, ifTrue, ifFalse)
    }

  lazy val prefixFunctionCall: PackratParser[Expression] =
    functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
      case name ~ _ ~ args ~ _ =>
        lookupCall(name, args: _*)
    }

  lazy val prefixFunctionCallOneArg: PackratParser[Expression] =
    functionIdent ~ "(" ~ expression ~ ")" ^^ {
      case name ~ _ ~ arg ~ _ =>
        lookupCall(name, arg)
    }

  lazy val prefixTrim: PackratParser[Expression] =
    TRIM ~ "(" ~ trimMode ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
      case _ ~ _ ~ mode ~ _ ~ trimCharacter ~ _ ~ operand ~ _ =>
        unresolvedCall(
          BuiltInFunctionDefinitions.TRIM,
          valueLiteral(mode == TRIM_MODE_LEADING.key || mode == TRIM_MODE_BOTH.key),
          valueLiteral(mode == TRIM_MODE_TRAILING.key || mode == TRIM_MODE_BOTH.key),
          trimCharacter,
          operand)
    }

  lazy val prefixTrimWithoutArgs: PackratParser[Expression] =
    TRIM ~ "(" ~ expression ~ ")" ^^ {
      case _ ~ _ ~ operand ~ _ =>
        unresolvedCall(
          BuiltInFunctionDefinitions.TRIM,
          valueLiteral(true),
          valueLiteral(true),
          valueLiteral(" "),
          operand)
    }

  lazy val prefixExtract: PackratParser[Expression] =
    EXTRACT ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
      case _ ~ _ ~ operand ~ _ ~ unit ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.EXTRACT, unit, operand)
      }

  lazy val prefixTimestampDiff: PackratParser[Expression] =
    TIMESTAMP_DIFF ~ "(" ~ timePointUnit ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
      case _ ~ _ ~ unit ~ _ ~ operand1 ~ _ ~ operand2 ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.TIMESTAMP_DIFF, unit, operand1, operand2)
    }

  lazy val prefixFloor: PackratParser[Expression] =
    FLOOR ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
      case _ ~ _ ~ operand ~ _ ~ unit ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.FLOOR, operand, unit)
    }

  lazy val prefixCeil: PackratParser[Expression] =
    CEIL ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
      case _ ~ _ ~ operand ~ _ ~ unit ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.CEIL, operand, unit)
    }

  lazy val prefixGet: PackratParser[Expression] =
    GET ~ "(" ~ composite ~ ","  ~ literalExpr ~ ")" ^^ {
      case _ ~ _ ~ e ~ _ ~ index ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.GET, e, index)
    }

  lazy val prefixFlattening: PackratParser[Expression] =
    FLATTEN ~ "(" ~> composite <~ ")" ^^ { e =>
      unresolvedCall(BuiltInFunctionDefinitions.FLATTEN, e)
    }

  lazy val prefixToDate: PackratParser[Expression] =
    TO_DATE ~ "(" ~> expression <~ ")" ^^ { e =>
      unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        e,
        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.DATE)))
    }

  lazy val prefixToTimestamp: PackratParser[Expression] =
    TO_TIMESTAMP ~ "(" ~> expression <~ ")" ^^ { e =>
      unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        e,
        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIMESTAMP)))
    }

  lazy val prefixToTime: PackratParser[Expression] =
    TO_TIME ~ "(" ~> expression <~ ")" ^^ { e =>
      unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        e,
        typeLiteral(fromLegacyInfoToDataType(SqlTimeTypeInfo.TIME)))
    }

  lazy val prefixDistinct: PackratParser[Expression] =
    functionIdent ~ "." ~ DISTINCT ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
      case name ~ _ ~ _ ~ _ ~ args ~ _ =>
        unresolvedCall(BuiltInFunctionDefinitions.DISTINCT, lookupCall(name, args: _*))
    }

  lazy val prefixAs: PackratParser[Expression] =
    AS ~ "(" ~ expression ~ "," ~ rep1sep(fieldReference, ",") ~ ")" ^^ {
      case _ ~ _ ~ e ~ _ ~ names ~ _ =>
        unresolvedCall(
          BuiltInFunctionDefinitions.AS,
          e :: names.map(n => valueLiteral(n.getName)): _*)
    }

  lazy val prefixed: PackratParser[Expression] =
    // expressions that need to be resolved early
    prefixFlattening |
    // expressions that need special expression conversion
    prefixAs| prefixToTimestamp | prefixToTime | prefixToDate |
    // expressions that take enumerations
    prefixCast | prefixTrim | prefixTrimWithoutArgs | prefixExtract | prefixFloor | prefixCeil |
      prefixTimestampDiff |
    // expressions that take literals
    prefixGet |
    // expression with special identifier
    prefixIf |
    // expression with prefix distinct
    prefixDistinct |
    // function call must always be at the end
    prefixFunctionCall | prefixFunctionCallOneArg

  // suffix/prefix composite

  lazy val composite: PackratParser[Expression] =
    over | suffixed | nullLiteral | prefixed | atom |
    failure("Composite expression expected.")

  // unary ops

  lazy val unaryNot: PackratParser[Expression] = "!" ~> composite ^^ { e =>
    unresolvedCall(BuiltInFunctionDefinitions.NOT, e)
  }

  lazy val unaryMinus: PackratParser[Expression] = "-" ~> composite ^^ { e =>
    unresolvedCall(BuiltInFunctionDefinitions.MINUS_PREFIX, e)
  }

  lazy val unaryPlus: PackratParser[Expression] = "+" ~> composite ^^ { e => e }

  lazy val unary: PackratParser[Expression] = composite | unaryNot | unaryMinus | unaryPlus |
    failure("Unary expression expected.")

  // arithmetic

  lazy val product: PackratParser[Expression] = unary * (
  "*" ^^^ {
    (a:Expression, b:Expression) => unresolvedCall(BuiltInFunctionDefinitions.TIMES, a, b)
  } | "/" ^^^ {
    (a:Expression, b:Expression) => unresolvedCall(BuiltInFunctionDefinitions.DIVIDE, a, b)
  } | "%" ^^^ {
    (a:Expression, b:Expression) => unresolvedCall(BuiltInFunctionDefinitions.MOD, a, b)
  }) | failure("Product expected.")

  lazy val term: PackratParser[Expression] = product * (
    "+" ^^^ {
      (a:Expression, b:Expression) => unresolvedCall(BuiltInFunctionDefinitions.PLUS, a, b)
    } | "-" ^^^ {
      (a:Expression, b:Expression) => unresolvedCall(BuiltInFunctionDefinitions.MINUS, a, b)
    }) | failure("Term expected.")

  // comparison

  lazy val equalTo: PackratParser[Expression] = term ~ ("===" | "==" | "=") ~ term ^^ {
    case l ~ _ ~ r => unresolvedCall(BuiltInFunctionDefinitions.EQUALS, l, r)
  }

  lazy val notEqualTo: PackratParser[Expression] = term ~ ("!==" | "!=" | "<>") ~ term ^^ {
    case l ~ _ ~ r => unresolvedCall(BuiltInFunctionDefinitions.NOT_EQUALS, l, r)
  }

  lazy val greaterThan: PackratParser[Expression] = term ~ ">" ~ term ^^ {
    case l ~ _ ~ r => unresolvedCall(BuiltInFunctionDefinitions.GREATER_THAN, l, r)
  }

  lazy val greaterThanOrEqual: PackratParser[Expression] = term ~ ">=" ~ term ^^ {
    case l ~ _ ~ r => unresolvedCall(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, l, r)
  }

  lazy val lessThan: PackratParser[Expression] = term ~ "<" ~ term ^^ {
    case l ~ _ ~ r => unresolvedCall(BuiltInFunctionDefinitions.LESS_THAN, l, r)
  }

  lazy val lessThanOrEqual: PackratParser[Expression] = term ~ "<=" ~ term ^^ {
    case l ~ _ ~ r => unresolvedCall(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, l, r)
  }

  lazy val comparison: PackratParser[Expression] =
    equalTo | notEqualTo |
    greaterThan | greaterThanOrEqual |
    lessThan | lessThanOrEqual | term |
    failure("Comparison expected.")

  // logic

  lazy val logic: PackratParser[Expression] = comparison * (
    "&&" ^^^ {
      (a:Expression, b:Expression) => unresolvedCall(BuiltInFunctionDefinitions.AND, a, b)
    } | "||" ^^^ {
      (a:Expression, b:Expression) => unresolvedCall(BuiltInFunctionDefinitions.OR, a, b)
    }) | failure("Logic expected.")

  // time indicators

  lazy val timeIndicator: PackratParser[Expression] = proctime | rowtime

  lazy val proctime: PackratParser[Expression] = fieldReference ~ "." ~ PROCTIME ^^ {
    case f ~ _ ~ _ => unresolvedCall(BuiltInFunctionDefinitions.PROCTIME, f)
  }

  lazy val rowtime: PackratParser[Expression] = fieldReference ~ "." ~ ROWTIME ^^ {
    case f ~ _ ~ _ => unresolvedCall(BuiltInFunctionDefinitions.ROWTIME, f)
  }

  // alias

  lazy val alias: PackratParser[Expression] = logic ~ AS ~ fieldReference ^^ {
      case e ~ _ ~ name =>
        unresolvedCall(BuiltInFunctionDefinitions.AS, e, valueLiteral(name.getName))
  } | logic ~ AS ~ "(" ~ rep1sep(fieldReference, ",") ~ ")" ^^ {
    case e ~ _ ~ _ ~ names ~ _ =>
      unresolvedCall(
        BuiltInFunctionDefinitions.AS,
        e :: names.map(n => valueLiteral(n.getName)): _*)
  } | logic

  lazy val aliasMapping: PackratParser[Expression] =
    fieldReference ~ AS ~ fieldReference ^^ {
      case e ~ _ ~ name =>
        unresolvedCall(BuiltInFunctionDefinitions.AS, e, valueLiteral(name.getName))
  }

  // columns

  lazy val fieldNameRange: PackratParser[Expression] = fieldReference ~ TO ~ fieldReference ^^ {
    case start ~ _ ~ end => unresolvedCall(BuiltInFunctionDefinitions.RANGE_TO, start, end)
  }

  lazy val fieldIndexRange: PackratParser[Expression] = numberLiteral ~ TO ~ numberLiteral ^^ {
    case start ~ _ ~ end => unresolvedCall(BuiltInFunctionDefinitions.RANGE_TO, start, end)
  }

  lazy val range = fieldNameRange | fieldIndexRange

  lazy val expression: PackratParser[Expression] = range | overConstant | alias |
    failure("Invalid expression.")

  lazy val expressionList: Parser[List[Expression]] = rep1sep(expression, ",")

  def parseExpressionList(expression: String): JList[Expression] = {
    parseAll(expressionList, expression) match {
      case Success(lst, _) => lst

      case NoSuccess(msg, next) =>
        throwError(msg, next)
    }
  }

  def parseExpression(exprString: String): Expression = {
    parseAll(expression, exprString) match {
      case Success(lst, _) => lst

      case NoSuccess(msg, next) =>
        throwError(msg, next)
    }
  }

  private def throwError(msg: String, next: Input): Nothing = {
    val improvedMsg = msg.replace("string matching regex `\\z'", "End of expression")

    throw new ExpressionParserException(
      s"""Could not parse expression at column ${next.pos.column}: $improvedMsg
        |${next.pos.longString}""".stripMargin)
  }
}
