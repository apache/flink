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
package org.apache.flink.api.table.expressions

import org.apache.calcite.avatica.util.DateTimeUtils.{MILLIS_PER_DAY, MILLIS_PER_HOUR, MILLIS_PER_MINUTE, MILLIS_PER_SECOND}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.table.ExpressionParserException
import org.apache.flink.api.table.expressions.ExpressionUtils.{toMilliInterval, toMonthInterval}
import org.apache.flink.api.table.expressions.TimeIntervalUnit.TimeIntervalUnit
import org.apache.flink.api.table.expressions.TimePointUnit.TimePointUnit
import org.apache.flink.api.table.expressions.TrimMode.TrimMode
import org.apache.flink.api.table.typeutils.IntervalTypeInfo

import scala.language.implicitConversions
import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}

/**
 * Parser for expressions inside a String. This parses exactly the same expressions that
 * would be accepted by the Scala Expression DSL.
 *
 * See [[org.apache.flink.api.scala.table.ImplicitExpressionConversions]] and
 * [[org.apache.flink.api.scala.table.ImplicitExpressionOperations]] for the constructs
 * available in the Scala Expression DSL. This parser must be kept in sync with the Scala DSL
 * lazy valined in the above files.
 */
object ExpressionParser extends JavaTokenParsers with PackratParsers {
  case class Keyword(key: String)

  // Convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }

  // Keyword

  lazy val AS: Keyword = Keyword("as")
  lazy val COUNT: Keyword = Keyword("count")
  lazy val AVG: Keyword = Keyword("avg")
  lazy val MIN: Keyword = Keyword("min")
  lazy val MAX: Keyword = Keyword("max")
  lazy val SUM: Keyword = Keyword("sum")
  lazy val CAST: Keyword = Keyword("cast")
  lazy val NULL: Keyword = Keyword("Null")
  lazy val IF: Keyword = Keyword("?")
  lazy val ASC: Keyword = Keyword("asc")
  lazy val DESC: Keyword = Keyword("desc")
  lazy val TO_DATE: Keyword = Keyword("toDate")
  lazy val TO_TIME: Keyword = Keyword("toTime")
  lazy val TO_TIMESTAMP: Keyword = Keyword("toTimestamp")
  lazy val TRIM: Keyword = Keyword("trim")
  lazy val EXTRACT: Keyword = Keyword("extract")
  lazy val FLOOR: Keyword = Keyword("floor")
  lazy val CEIL: Keyword = Keyword("ceil")
  lazy val YEAR: Keyword = Keyword("year")
  lazy val MONTH: Keyword = Keyword("month")
  lazy val DAY: Keyword = Keyword("day")
  lazy val HOUR: Keyword = Keyword("hour")
  lazy val MINUTE: Keyword = Keyword("minute")
  lazy val SECOND: Keyword = Keyword("second")
  lazy val MILLI: Keyword = Keyword("milli")
  lazy val STAR: Keyword = Keyword("*")

  def functionIdent: ExpressionParser.Parser[String] =
    not(AS) ~ not(COUNT) ~ not(AVG) ~ not(MIN) ~ not(MAX) ~
      not(SUM) ~ not(CAST) ~ not(NULL) ~
      not(IF) ~> super.ident

  // symbols

  lazy val timeIntervalUnit: PackratParser[Expression] = TimeIntervalUnit.values map {
    case unit: TimeIntervalUnit => literal(unit.toString) ^^^ unit.toExpr
  } reduceLeft(_ | _)

  lazy val timePointUnit: PackratParser[Expression] = TimePointUnit.values map {
    case unit: TimePointUnit => literal(unit.toString) ^^^ unit.toExpr
  } reduceLeft(_ | _)

  lazy val trimMode: PackratParser[Expression] = TrimMode.values map {
    case mode: TrimMode => literal(mode.toString) ^^^ mode.toExpr
  } reduceLeft(_ | _)

  // data types

  lazy val dataType: PackratParser[TypeInformation[_]] =
    "BYTE" ^^ { ti => BasicTypeInfo.BYTE_TYPE_INFO } |
      "SHORT" ^^ { ti => BasicTypeInfo.SHORT_TYPE_INFO } |
      "INTERVAL_MONTHS" ^^ {
        ti => IntervalTypeInfo.INTERVAL_MONTHS.asInstanceOf[TypeInformation[_]]
      } |
      "INTERVAL_MILLIS" ^^ {
        ti => IntervalTypeInfo.INTERVAL_MILLIS.asInstanceOf[TypeInformation[_]]
      } |
      "INT" ^^ { ti => BasicTypeInfo.INT_TYPE_INFO } |
      "LONG" ^^ { ti => BasicTypeInfo.LONG_TYPE_INFO } |
      "FLOAT" ^^ { ti => BasicTypeInfo.FLOAT_TYPE_INFO } |
      "DOUBLE" ^^ { ti => BasicTypeInfo.DOUBLE_TYPE_INFO } |
      ("BOOL" | "BOOLEAN" ) ^^ { ti => BasicTypeInfo.BOOLEAN_TYPE_INFO } |
      "STRING" ^^ { ti => BasicTypeInfo.STRING_TYPE_INFO } |
      "DATE" ^^ { ti => SqlTimeTypeInfo.DATE.asInstanceOf[TypeInformation[_]] } |
      "TIMESTAMP" ^^ { ti => SqlTimeTypeInfo.TIMESTAMP } |
      "TIME" ^^ { ti => SqlTimeTypeInfo.TIME } |
      "DECIMAL" ^^ { ti => BasicTypeInfo.BIG_DEC_TYPE_INFO }

  // Literals

  // same as floatingPointNumber but we do not allow trailing dot "12.d" or "2."
  lazy val floatingPointNumberFlink: Parser[String] =
    """-?(\d+(\.\d+)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r

  lazy val numberLiteral: PackratParser[Expression] =
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

  lazy val singleQuoteStringLiteral: Parser[Expression] =
    ("'" + """([^'\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r ^^ {
      str => Literal(str.substring(1, str.length - 1))
    }

  lazy val stringLiteralFlink: PackratParser[Expression] = super.stringLiteral ^^ {
    str => Literal(str.substring(1, str.length - 1))
  }

  lazy val boolLiteral: PackratParser[Expression] = ("true" | "false") ^^ {
    str => Literal(str.toBoolean)
  }

  lazy val nullLiteral: PackratParser[Expression] = NULL ~ "(" ~> dataType <~ ")" ^^ {
    dt => Null(dt)
  }

  lazy val literalExpr: PackratParser[Expression] =
    numberLiteral |
      stringLiteralFlink | singleQuoteStringLiteral |
      boolLiteral | nullLiteral

  lazy val fieldReference: PackratParser[NamedExpression] = (STAR | ident) ^^ {
    sym => UnresolvedFieldReference(sym)
  }

  lazy val atom: PackratParser[Expression] =
    ( "(" ~> expression <~ ")" ) | literalExpr | fieldReference

  // suffix operators

  lazy val suffixSum: PackratParser[Expression] =
    composite <~ "." ~ SUM ~ opt("()") ^^ { e => Sum(e) }

  lazy val suffixMin: PackratParser[Expression] =
    composite <~ "." ~ MIN ~ opt("()") ^^ { e => Min(e) }

  lazy val suffixMax: PackratParser[Expression] =
    composite <~ "." ~ MAX ~ opt("()") ^^ { e => Max(e) }

  lazy val suffixCount: PackratParser[Expression] =
    composite <~ "." ~ COUNT ~ opt("()") ^^ { e => Count(e) }

  lazy val suffixAvg: PackratParser[Expression] =
    composite <~ "." ~ AVG ~ opt("()") ^^ { e => Avg(e) }

  lazy val suffixCast: PackratParser[Expression] =
    composite ~ "." ~ CAST ~ "(" ~ dataType ~ ")" ^^ {
    case e ~ _ ~ _ ~ _ ~ dt ~ _ => Cast(e, dt)
  }

  lazy val suffixAs: PackratParser[Expression] =
    composite ~ "." ~ AS ~ "(" ~ fieldReference ~ ")" ^^ {
    case e ~ _ ~ _ ~ _ ~ target ~ _ => Alias(e, target.name)
  }

  lazy val suffixTrim = composite ~ "." ~ TRIM ~ "(" ~ trimMode ~ "," ~ expression ~ ")" ^^ {
    case operand ~ _ ~ _ ~ _ ~ mode ~ _ ~ trimCharacter ~ _ => Trim(mode, trimCharacter, operand)
  }

  lazy val suffixTrimWithoutArgs = composite <~ "." ~ TRIM ~ opt("()") ^^ {
    e => Trim(TrimMode.BOTH, TrimConstants.TRIM_DEFAULT_CHAR, e)
  }

  lazy val suffixIf: PackratParser[Expression] =
    composite ~ "." ~ IF ~ "(" ~ expression ~ "," ~ expression ~ ")" ^^ {
    case condition ~ _ ~ _ ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ => If(condition, ifTrue, ifFalse)
  }

  lazy val suffixExtract = composite ~ "." ~ EXTRACT ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
    case operand ~ _  ~ _ ~ _ ~ unit ~ _ => Extract(unit, operand)
  }

  lazy val suffixFloor = composite ~ "." ~ FLOOR ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
    case operand ~ _  ~ _ ~ _ ~ unit ~ _ => TemporalFloor(unit, operand)
  }

  lazy val suffixCeil = composite ~ "." ~ CEIL ~ "(" ~ timeIntervalUnit ~ ")" ^^ {
    case operand ~ _  ~ _ ~ _ ~ unit ~ _ => TemporalCeil(unit, operand)
  }

  lazy val suffixFunctionCall =
    composite ~ "." ~ functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
    case operand ~ _ ~ name ~ _ ~ args ~ _ => Call(name.toUpperCase, operand :: args)
  }

  lazy val suffixFunctionCallOneArg = composite ~ "." ~ functionIdent ^^ {
    case operand ~ _ ~ name => Call(name.toUpperCase, Seq(operand))
  }

  lazy val suffixAsc : PackratParser[Expression] =
    atom <~ "." ~ ASC ~ opt("()") ^^ { e => Asc(e) }

  lazy val suffixDesc : PackratParser[Expression] =
    atom <~ "." ~ DESC ~ opt("()") ^^ { e => Desc(e) }

  lazy val suffixToDate: PackratParser[Expression] =
    composite <~ "." ~ TO_DATE ~ opt("()") ^^ { e => Cast(e, SqlTimeTypeInfo.DATE) }

  lazy val suffixToTimestamp: PackratParser[Expression] =
    composite <~ "." ~ TO_TIMESTAMP ~ opt("()") ^^ { e => Cast(e, SqlTimeTypeInfo.TIMESTAMP) }

  lazy val suffixToTime: PackratParser[Expression] =
    composite <~ "." ~ TO_TIME ~ opt("()") ^^ { e => Cast(e, SqlTimeTypeInfo.TIME) }

  lazy val suffixTimeInterval : PackratParser[Expression] =
    composite ~ "." ~ (YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLI) ^^ {

    case expr ~ _ ~ YEAR.key => toMonthInterval(expr, 12)

    case expr ~ _ ~ MONTH.key => toMonthInterval(expr, 1)

    case expr ~ _ ~ DAY.key => toMilliInterval(expr, MILLIS_PER_DAY)

    case expr ~ _ ~ HOUR.key => toMilliInterval(expr, MILLIS_PER_HOUR)

    case expr ~ _ ~ MINUTE.key => toMilliInterval(expr, MILLIS_PER_MINUTE)

    case expr ~ _ ~ SECOND.key => toMilliInterval(expr, MILLIS_PER_SECOND)

    case expr ~ _ ~ MILLI.key => toMilliInterval(expr, 1)
  }

  lazy val suffixed: PackratParser[Expression] =
    suffixTimeInterval | suffixSum | suffixMin | suffixMax |
      suffixCount | suffixAvg | suffixCast | suffixAs | suffixTrim | suffixTrimWithoutArgs |
      suffixIf | suffixAsc | suffixDesc | suffixToDate | suffixToTimestamp | suffixToTime |
      suffixExtract | suffixFloor | suffixCeil |
      suffixFunctionCall | suffixFunctionCallOneArg // function call must always be at the end

  // prefix operators

  lazy val prefixSum: PackratParser[Expression] =
    SUM ~ "(" ~> expression <~ ")" ^^ { e => Sum(e) }

  lazy val prefixMin: PackratParser[Expression] =
    MIN ~ "(" ~> expression <~ ")" ^^ { e => Min(e) }

  lazy val prefixMax: PackratParser[Expression] =
    MAX ~ "(" ~> expression <~ ")" ^^ { e => Max(e) }

  lazy val prefixCount: PackratParser[Expression] =
    COUNT ~ "(" ~> expression <~ ")" ^^ { e => Count(e) }

  lazy val prefixAvg: PackratParser[Expression] =
    AVG ~ "(" ~> expression <~ ")" ^^ { e => Avg(e) }

  lazy val prefixCast: PackratParser[Expression] =
    CAST ~ "(" ~ expression ~ "," ~ dataType ~ ")" ^^ {
    case _ ~ _ ~ e ~ _ ~ dt ~ _ => Cast(e, dt)
  }

  lazy val prefixAs: PackratParser[Expression] =
    AS ~ "(" ~ expression ~ "," ~ fieldReference ~ ")" ^^ {
    case _ ~ _ ~ e ~ _ ~ target ~ _ => Alias(e, target.name)
  }

  lazy val prefixIf: PackratParser[Expression] =
      IF ~ "(" ~ expression ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
    case _ ~ _ ~ condition ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ => If(condition, ifTrue, ifFalse)
  }

  lazy val prefixFunctionCall = functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
    case name ~ _ ~ args ~ _ => Call(name.toUpperCase, args)
  }

  lazy val prefixFunctionCallOneArg = functionIdent ~ "(" ~ expression ~ ")" ^^ {
    case name ~ _ ~ arg ~ _ => Call(name.toUpperCase, Seq(arg))
  }

  lazy val prefixTrim = TRIM ~ "(" ~ trimMode ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
    case _ ~ _ ~ mode ~ _ ~ trimCharacter ~ _ ~ operand ~ _ => Trim(mode, trimCharacter, operand)
  }

  lazy val prefixTrimWithoutArgs = TRIM ~ "(" ~ expression ~ ")" ^^ {
    case _ ~ _ ~ operand ~ _ => Trim(TrimMode.BOTH, TrimConstants.TRIM_DEFAULT_CHAR, operand)
  }

  lazy val prefixExtract = EXTRACT ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
    case _ ~ _ ~ operand ~ _ ~ unit ~ _ => Extract(unit, operand)
  }

  lazy val prefixFloor = FLOOR ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
    case _ ~ _ ~ operand ~ _ ~ unit ~ _ => TemporalFloor(unit, operand)
  }

  lazy val prefixCeil = CEIL ~ "(" ~ expression ~ "," ~ timeIntervalUnit ~ ")" ^^ {
    case _ ~ _ ~ operand ~ _ ~ unit ~ _ => TemporalCeil(unit, operand)
  }

  lazy val prefixed: PackratParser[Expression] =
    prefixSum | prefixMin | prefixMax | prefixCount | prefixAvg |
      prefixCast | prefixAs | prefixTrim | prefixTrimWithoutArgs | prefixIf | prefixExtract |
      prefixFloor | prefixCeil |
      prefixFunctionCall | prefixFunctionCallOneArg // function call must always be at the end

  // suffix/prefix composite

  lazy val composite: PackratParser[Expression] = suffixed | prefixed | atom

  // unary ops

  lazy val unaryNot: PackratParser[Expression] = "!" ~> composite ^^ { e => Not(e) }

  lazy val unaryMinus: PackratParser[Expression] = "-" ~> composite ^^ { e => UnaryMinus(e) }

  lazy val unary = composite | unaryNot | unaryMinus

  // arithmetic

  lazy val product = unary * (
    "*" ^^^ { (a:Expression, b:Expression) => Mul(a,b) } |
      "/" ^^^ { (a:Expression, b:Expression) => Div(a,b) } |
      "%" ^^^ { (a:Expression, b:Expression) => Mod(a,b) } )

  lazy val term = product * (
    "+" ^^^ { (a:Expression, b:Expression) => Plus(a,b) } |
     "-" ^^^ { (a:Expression, b:Expression) => Minus(a,b) } )

  // Comparison

  lazy val equalTo: PackratParser[Expression] = term ~ ("===" | "=") ~ term ^^ {
    case l ~ _ ~ r => EqualTo(l, r)
  }

  lazy val notEqualTo: PackratParser[Expression] = term ~ ("!==" | "!=" | "<>") ~ term ^^ {
    case l ~ _ ~ r => NotEqualTo(l, r)
  }

  lazy val greaterThan: PackratParser[Expression] = term ~ ">" ~ term ^^ {
    case l ~ _ ~ r => GreaterThan(l, r)
  }

  lazy val greaterThanOrEqual: PackratParser[Expression] = term ~ ">=" ~ term ^^ {
    case l ~ _ ~ r => GreaterThanOrEqual(l, r)
  }

  lazy val lessThan: PackratParser[Expression] = term ~ "<" ~ term ^^ {
    case l ~ _ ~ r => LessThan(l, r)
  }

  lazy val lessThanOrEqual: PackratParser[Expression] = term ~ "<=" ~ term ^^ {
    case l ~ _ ~ r => LessThanOrEqual(l, r)
  }

  lazy val comparison: PackratParser[Expression] =
      equalTo | notEqualTo |
      greaterThan | greaterThanOrEqual |
      lessThan | lessThanOrEqual | term

  // logic

  lazy val logic = comparison * (
    "&&" ^^^ { (a:Expression, b:Expression) => And(a,b) } |
      "||" ^^^ { (a:Expression, b:Expression) => Or(a,b) } )

  // alias

  lazy val alias: PackratParser[Expression] = logic ~ AS ~ fieldReference ^^ {
    case e ~ _ ~ name => Alias(e, name.name)
  } | logic

  lazy val expression: PackratParser[Expression] = alias

  lazy val expressionList: Parser[List[Expression]] = rep1sep(expression, ",")

  def parseExpressionList(expression: String): List[Expression] = {
    parseAll(expressionList, expression) match {
      case Success(lst, _) => lst

      case Failure(msg, _) => throw ExpressionParserException(
        "Could not parse expression: " + msg)

      case Error(msg, _) => throw ExpressionParserException(
        "Could not parse expression: " + msg)
    }
  }

  def parseExpression(exprString: String): Expression = {
    parseAll(expression, exprString) match {
      case Success(lst, _) => lst

      case fail =>
        throw ExpressionParserException("Could not parse expression: " + fail.toString)
    }
  }
}
