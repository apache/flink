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

import org.apache.calcite.avatica.util.DateTimeUtils.{MILLIS_PER_DAY, MILLIS_PER_HOUR, MILLIS_PER_MINUTE, MILLIS_PER_SECOND}
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.PlannerExpressionUtils.{toMilliInterval, toMonthInterval}

import _root_.scala.language.implicitConversions
import _root_.scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}

/**
 * Parser for expressions inside a String. This parses exactly the same expressions that
 * would be accepted by the Scala Expression DSL.
 *
 * See [[org.apache.flink.table.api.scala.ImplicitExpressionConversions]] and
 * [[org.apache.flink.table.api.scala.ImplicitExpressionOperations]] for the constructs
 * available in the Scala Expression DSL. This parser must be kept in sync with the Scala DSL
 * lazy valined in the above files.
 */
object ExpressionParser extends JavaTokenParsers with PackratParsers {
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

  def functionIdent: ExpressionParser.Parser[String] = super.ident

  // symbols

  lazy val timeIntervalUnit: PackratParser[PlannerExpression] = TimeIntervalUnit.values map {
    unit: TimeIntervalUnit => literal(unit.toString) ^^^ SymbolPlannerExpression(unit)
  } reduceLeft(_ | _)

  lazy val timePointUnit: PackratParser[PlannerExpression] = TimePointUnit.values map {
    unit: TimePointUnit => literal(unit.toString) ^^^ SymbolPlannerExpression(unit)
  } reduceLeft(_ | _)

  lazy val trimMode: PackratParser[PlannerExpression] = TrimMode.values map {
    mode: TrimMode => literal(mode.toString) ^^^ SymbolPlannerExpression(mode)
  } reduceLeft(_ | _)

  lazy val currentRange: PackratParser[PlannerExpression] = CURRENT_RANGE ^^ {
    _ => CurrentRange()
  }

  lazy val currentRow: PackratParser[PlannerExpression] = CURRENT_ROW ^^ {
    _ => CurrentRow()
  }

  lazy val unboundedRange: PackratParser[PlannerExpression] = UNBOUNDED_RANGE ^^ {
    _ => UnboundedRange()
  }

  lazy val unboundedRow: PackratParser[PlannerExpression] = UNBOUNDED_ROW ^^ {
    _ => UnboundedRow()
  }

  lazy val overConstant: PackratParser[PlannerExpression] =
    currentRange | currentRow | unboundedRange | unboundedRow

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

  lazy val numberLiteral: PackratParser[PlannerExpression] =
    (wholeNumber <~ ("l" | "L")) ^^ { n => Literal(n.toLong) } |
      (decimalNumber <~ ("p" | "P")) ^^ { n => Literal(BigDecimal(n)) } |
      (floatingPointNumberFlink | decimalNumber) ^^ {
        n =>
          if (n.matches("""-?\d+""")) {
            Literal(n.toInt)
          } else if (n.endsWith("f") || n.endsWith("F")) {
            Literal(n.toFloat)
          } else {
            Literal(n.toDouble)
          }
      }

  // string with single quotes such as 'It''s me.'
  lazy val singleQuoteStringLiteral: Parser[PlannerExpression] = "'(?:''|[^'])*'".r ^^ {
    str =>
      val escaped = str.substring(1, str.length - 1).replace("''", "'")
      Literal(escaped)
  }

  // string with double quotes such as "I ""like"" dogs."
  lazy val doubleQuoteStringLiteral: PackratParser[PlannerExpression] = "\"(?:\"\"|[^\"])*\"".r ^^ {
    str =>
      val escaped = str.substring(1, str.length - 1).replace("\"\"", "\"")
      Literal(escaped)
  }

  lazy val boolLiteral: PackratParser[PlannerExpression] = (TRUE | FALSE) ^^ {
    str => Literal(str.toBoolean)
  }

  lazy val nullLiteral: PackratParser[PlannerExpression] = NULL ~ "(" ~> dataType <~ ")" ^^ {
    dt => Null(dt)
  }

  lazy val literalExpr: PackratParser[PlannerExpression] =
    numberLiteral | doubleQuoteStringLiteral | singleQuoteStringLiteral | boolLiteral

  lazy val fieldReference: PackratParser[NamedExpression] = (STAR | ident) ^^ {
    sym => UnresolvedFieldReference(sym)
  }

  lazy val atom: PackratParser[PlannerExpression] =
    ( "(" ~> expression <~ ")" ) | (fieldReference ||| literalExpr)

  lazy val over: PackratParser[PlannerExpression] = composite ~ OVER ~ fieldReference ^^ {
    case agg ~ _ ~ windowRef => UnresolvedOverCall(agg, windowRef)
  }

  // suffix operators

  lazy val suffixAsc : PackratParser[PlannerExpression] =
    composite <~ "." ~ ASC ~ opt("()") ^^ { e => Asc(e) }

  lazy val suffixDesc : PackratParser[PlannerExpression] =
    composite <~ "." ~ DESC ~ opt("()") ^^ { e => Desc(e) }

  lazy val suffixCast: PackratParser[PlannerExpression] =
    composite ~ "." ~ CAST ~ "(" ~ dataType ~ ")" ^^ {
    case e ~ _ ~ _ ~ _ ~ dt ~ _ => Cast(e, dt)
  }

  lazy val suffixTrim: PackratParser[PlannerExpression] =
    composite ~ "." ~ TRIM ~ "(" ~ trimMode ~ "," ~ expression ~ ")" ^^ {
      case operand ~ _ ~ _ ~ _ ~ mode ~ _ ~ trimCharacter ~ _ => Trim(mode, trimCharacter, operand)
    }

  lazy val suffixTrimWithoutArgs: PackratParser[PlannerExpression] =
    composite <~ "." ~ TRIM ~ opt("()") ^^ {
      e => Trim(SymbolPlannerExpression(TrimMode.BOTH), TrimConstants.TRIM_DEFAULT_CHAR, e)
    }

  lazy val suffixIf: PackratParser[PlannerExpression] =
    composite ~ "." ~ IF ~ "(" ~ expression ~ "," ~ expression ~ ")" ^^ {
    case condition ~ _ ~ _ ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ => If(condition, ifTrue, ifFalse)
  }

  lazy val suffixExtract: PackratParser[PlannerExpression] =
    composite ~ "." ~ EXTRACT ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
      case operand ~ _  ~ _ ~ _ ~ unit ~ _ => Extract(unit, operand)
    }

  lazy val suffixFloor: PackratParser[PlannerExpression] =
    composite ~ "." ~ FLOOR ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
      case operand ~ _  ~ _ ~ _ ~ unit ~ _ => TemporalFloor(unit, operand)
    }

  lazy val suffixCeil: PackratParser[PlannerExpression] =
    composite ~ "." ~ CEIL ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
      case operand ~ _  ~ _ ~ _ ~ unit ~ _ => TemporalCeil(unit, operand)
    }

  // required because op.log(base) changes order of a parameters
  lazy val suffixLog: PackratParser[PlannerExpression] =
    composite ~ "." ~ LOG ~ "(" ~ expression ~ ")" ^^ {
      case operand ~ _ ~ _ ~ _ ~ base ~ _ => Log(base, operand)
    }

  lazy val suffixFunctionCall: PackratParser[PlannerExpression] =
    composite ~ "." ~ functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
    case operand ~ _ ~ name ~ _ ~ args ~ _ => Call(name.toUpperCase, operand :: args)
  }

  lazy val suffixFunctionCallOneArg: PackratParser[PlannerExpression] =
    composite ~ "." ~ functionIdent ^^ {
      case operand ~ _ ~ name => Call(name.toUpperCase, Seq(operand))
    }

  lazy val suffixToDate: PackratParser[PlannerExpression] =
    composite <~ "." ~ TO_DATE ~ opt("()") ^^ { e => Cast(e, SqlTimeTypeInfo.DATE) }

  lazy val suffixToTimestamp: PackratParser[PlannerExpression] =
    composite <~ "." ~ TO_TIMESTAMP ~ opt("()") ^^ {
      e => Cast(e, SqlTimeTypeInfo.TIMESTAMP) }

  lazy val suffixToTime: PackratParser[PlannerExpression] =
    composite <~ "." ~ TO_TIME ~ opt("()") ^^ { e => Cast(e, SqlTimeTypeInfo.TIME) }

  lazy val suffixTimeInterval : PackratParser[PlannerExpression] =
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

  lazy val suffixRowInterval : PackratParser[PlannerExpression] =
    composite <~ "." ~ ROWS ^^ { e => PlannerExpressionUtils.toRowInterval(e) }

  lazy val suffixGet: PackratParser[PlannerExpression] =
    composite ~ "." ~ GET ~ "(" ~ literalExpr ~ ")" ^^ {
      case e ~ _ ~ _ ~ _ ~ index ~ _ =>
        GetCompositeField(e, index.asInstanceOf[Literal].value)
  }

  lazy val suffixFlattening: PackratParser[PlannerExpression] =
    composite <~ "." ~ FLATTEN ~ opt("()") ^^ { e => Flattening(e) }

  lazy val suffixDistinct: PackratParser[PlannerExpression] =
    composite <~ "." ~ DISTINCT ~ opt("()") ^^ { e => DistinctAgg(e) }

  lazy val suffixAs: PackratParser[PlannerExpression] =
    composite ~ "." ~ AS ~ "(" ~ rep1sep(fieldReference, ",") ~ ")" ^^ {
      case e ~ _ ~ _ ~ _ ~ target ~ _ => Alias(e, target.head.name, target.tail.map(_.name))
  }

  lazy val suffixed: PackratParser[PlannerExpression] =
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

  lazy val prefixCast: PackratParser[PlannerExpression] =
    CAST ~ "(" ~ expression ~ "," ~ dataType ~ ")" ^^ {
    case _ ~ _ ~ e ~ _ ~ dt ~ _ => Cast(e, dt)
  }

  lazy val prefixIf: PackratParser[PlannerExpression] =
      IF ~ "(" ~ expression ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
    case _ ~ _ ~ condition ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ => If(condition, ifTrue, ifFalse)
  }

  lazy val prefixFunctionCall: PackratParser[PlannerExpression] =
    functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
      case name ~ _ ~ args ~ _ => Call(name.toUpperCase, args)
    }

  lazy val prefixFunctionCallOneArg: PackratParser[PlannerExpression] =
    functionIdent ~ "(" ~ expression ~ ")" ^^ {
      case name ~ _ ~ arg ~ _ => Call(name.toUpperCase, Seq(arg))
    }

  lazy val prefixTrim: PackratParser[PlannerExpression] =
    TRIM ~ "(" ~ trimMode ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
      case _ ~ _ ~ mode ~ _ ~ trimCharacter ~ _ ~ operand ~ _ => Trim(mode, trimCharacter, operand)
    }

  lazy val prefixTrimWithoutArgs: PackratParser[PlannerExpression] =
    TRIM ~ "(" ~ expression ~ ")" ^^ {
    case _ ~ _ ~ operand ~ _ =>
      Trim(SymbolPlannerExpression(TrimMode.BOTH), TrimConstants.TRIM_DEFAULT_CHAR, operand)
  }

  lazy val prefixExtract: PackratParser[PlannerExpression] =
    EXTRACT ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
      case _ ~ _ ~ operand ~ _ ~ unit ~ _ => Extract(unit, operand)
    }

  lazy val prefixTimestampDiff: PackratParser[PlannerExpression] =
    TIMESTAMP_DIFF ~ "(" ~ timePointUnit ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
      case _ ~ _ ~ unit ~ _ ~ operand1 ~ _ ~ operand2 ~ _ => TimestampDiff(unit, operand1, operand2)
    }

  lazy val prefixFloor: PackratParser[PlannerExpression] =
    FLOOR ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
      case _ ~ _ ~ operand ~ _ ~ unit ~ _ => TemporalFloor(unit, operand)
    }

  lazy val prefixCeil: PackratParser[PlannerExpression] =
    CEIL ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
      case _ ~ _ ~ operand ~ _ ~ unit ~ _ => TemporalCeil(unit, operand)
    }

  lazy val prefixGet: PackratParser[PlannerExpression] =
    GET ~ "(" ~ composite ~ ","  ~ literalExpr ~ ")" ^^ {
      case _ ~ _ ~ e ~ _ ~ index ~ _ =>
        GetCompositeField(e, index.asInstanceOf[Literal].value)
  }

  lazy val prefixFlattening: PackratParser[PlannerExpression] =
    FLATTEN ~ "(" ~> composite <~ ")" ^^ { e => Flattening(e) }

  lazy val prefixToDate: PackratParser[PlannerExpression] =
    TO_DATE ~ "(" ~> expression <~ ")" ^^ { e => Cast(e, SqlTimeTypeInfo.DATE) }

  lazy val prefixToTimestamp: PackratParser[PlannerExpression] =
    TO_TIMESTAMP ~ "(" ~> expression <~ ")" ^^ { e => Cast(e, SqlTimeTypeInfo.TIMESTAMP) }

  lazy val prefixToTime: PackratParser[PlannerExpression] =
    TO_TIME ~ "(" ~> expression <~ ")" ^^ { e => Cast(e, SqlTimeTypeInfo.TIME) }

  lazy val prefixDistinct: PackratParser[PlannerExpression] =
    functionIdent ~ "." ~ DISTINCT ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
      case name ~ _ ~ _ ~ _ ~ args ~ _ => DistinctAgg(Call(name.toUpperCase, args))
  }

  lazy val prefixAs: PackratParser[PlannerExpression] =
    AS ~ "(" ~ expression ~ "," ~ rep1sep(fieldReference, ",") ~ ")" ^^ {
    case _ ~ _ ~ e ~ _ ~ target ~ _ => Alias(e, target.head.name, target.tail.map(_.name))
  }

  lazy val prefixed: PackratParser[PlannerExpression] =
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

  lazy val composite: PackratParser[PlannerExpression] =
    over | suffixed | nullLiteral | prefixed | atom |
    failure("Composite expression expected.")

  // unary ops

  lazy val unaryNot: PackratParser[PlannerExpression] = "!" ~> composite ^^ { e => Not(e) }

  lazy val unaryMinus: PackratParser[PlannerExpression] = "-" ~> composite ^^ { e => UnaryMinus(e) }

  lazy val unaryPlus: PackratParser[PlannerExpression] = "+" ~> composite ^^ { e => e }

  lazy val unary: PackratParser[PlannerExpression] = composite | unaryNot | unaryMinus | unaryPlus |
    failure("Unary expression expected.")

  // arithmetic

  lazy val product: PackratParser[PlannerExpression] = unary * (
    "*" ^^^ { (a:PlannerExpression, b:PlannerExpression) => Mul(a,b) } |
    "/" ^^^ { (a:PlannerExpression, b:PlannerExpression) => Div(a,b) } |
    "%" ^^^ { (a:PlannerExpression, b:PlannerExpression) => Mod(a,b) } ) |
    failure("Product expected.")

  lazy val term: PackratParser[PlannerExpression] = product * (
    "+" ^^^ { (a:PlannerExpression, b:PlannerExpression) => Plus(a,b) } |
    "-" ^^^ { (a:PlannerExpression, b:PlannerExpression) => Minus(a,b) } ) |
    failure("Term expected.")

  // comparison

  lazy val equalTo: PackratParser[PlannerExpression] = term ~ ("===" | "==" | "=") ~ term ^^ {
    case l ~ _ ~ r => EqualTo(l, r)
  }

  lazy val notEqualTo: PackratParser[PlannerExpression] = term ~ ("!==" | "!=" | "<>") ~ term ^^ {
    case l ~ _ ~ r => NotEqualTo(l, r)
  }

  lazy val greaterThan: PackratParser[PlannerExpression] = term ~ ">" ~ term ^^ {
    case l ~ _ ~ r => GreaterThan(l, r)
  }

  lazy val greaterThanOrEqual: PackratParser[PlannerExpression] = term ~ ">=" ~ term ^^ {
    case l ~ _ ~ r => GreaterThanOrEqual(l, r)
  }

  lazy val lessThan: PackratParser[PlannerExpression] = term ~ "<" ~ term ^^ {
    case l ~ _ ~ r => LessThan(l, r)
  }

  lazy val lessThanOrEqual: PackratParser[PlannerExpression] = term ~ "<=" ~ term ^^ {
    case l ~ _ ~ r => LessThanOrEqual(l, r)
  }

  lazy val comparison: PackratParser[PlannerExpression] =
    equalTo | notEqualTo |
    greaterThan | greaterThanOrEqual |
    lessThan | lessThanOrEqual | term |
    failure("Comparison expected.")

  // logic

  lazy val logic: PackratParser[PlannerExpression] = comparison * (
    "&&" ^^^ { (a:PlannerExpression, b:PlannerExpression) => And(a,b) } |
    "||" ^^^ { (a:PlannerExpression, b:PlannerExpression) => Or(a,b) } ) |
    failure("Logic expected.")

  // time indicators

  lazy val timeIndicator: PackratParser[PlannerExpression] = proctime | rowtime

  lazy val proctime: PackratParser[PlannerExpression] = fieldReference ~ "." ~ PROCTIME ^^ {
    case f ~ _ ~ _ => ProctimeAttribute(f)
  }

  lazy val rowtime: PackratParser[PlannerExpression] = fieldReference ~ "." ~ ROWTIME ^^ {
    case f ~ _ ~ _ => RowtimeAttribute(f)
  }

  // alias

  lazy val alias: PackratParser[PlannerExpression] = logic ~ AS ~ fieldReference ^^ {
      case e ~ _ ~ name => Alias(e, name.name)
  } | logic ~ AS ~ "(" ~ rep1sep(fieldReference, ",") ~ ")" ^^ {
    case e ~ _ ~ _ ~ names ~ _ => Alias(e, names.head.name, names.tail.map(_.name))
  } | logic

  lazy val aliasMapping: PackratParser[PlannerExpression] =
    fieldReference ~ AS ~ fieldReference ^^ {
      case e ~ _ ~ name => Alias(e, name.name)
  }

  lazy val expression: PackratParser[PlannerExpression] = overConstant | alias |
    failure("Invalid expression.")

  lazy val expressionList: Parser[List[PlannerExpression]] = rep1sep(expression, ",")

  def parseExpressionList(expression: String): List[PlannerExpression] = {
    parseAll(expressionList, expression) match {
      case Success(lst, _) => lst

      case NoSuccess(msg, next) =>
        throwError(msg, next)
    }
  }

  def parseExpression(exprString: String): PlannerExpression = {
    parseAll(expression, exprString) match {
      case Success(lst, _) => lst

      case NoSuccess(msg, next) =>
        throwError(msg, next)
    }
  }

  private def throwError(msg: String, next: Input): Nothing = {
    val improvedMsg = msg.replace("string matching regex `\\z'", "End of expression")

    throw ExpressionParserException(
      s"""Could not parse expression at column ${next.pos.column}: $improvedMsg
        |${next.pos.longString}""".stripMargin)
  }
}
