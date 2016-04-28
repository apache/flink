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

import org.apache.flink.api.common.typeinfo.{TypeInformation, BasicTypeInfo}
import org.apache.flink.api.table.ExpressionParserException

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
  lazy val IS_NULL: Keyword = Keyword("isNull")
  lazy val IS_NOT_NULL: Keyword = Keyword("isNotNull")
  lazy val CAST: Keyword = Keyword("cast")
  lazy val NULL: Keyword = Keyword("Null")
  lazy val EVAL: Keyword = Keyword("eval")
  lazy val ASC: Keyword = Keyword("asc")
  lazy val DESC: Keyword = Keyword("desc")

  def functionIdent: ExpressionParser.Parser[String] =
    not(AS) ~ not(COUNT) ~ not(AVG) ~ not(MIN) ~ not(MAX) ~
      not(SUM) ~ not(IS_NULL) ~ not(IS_NOT_NULL) ~ not(CAST) ~ not(NULL) ~
      not(EVAL) ~> super.ident

  // data types

  lazy val dataType: PackratParser[TypeInformation[_]] =
    "BYTE" ^^ { ti => BasicTypeInfo.BYTE_TYPE_INFO } |
      "SHORT" ^^ { ti => BasicTypeInfo.SHORT_TYPE_INFO } |
      "INT" ^^ { ti => BasicTypeInfo.INT_TYPE_INFO } |
      "LONG" ^^ { ti => BasicTypeInfo.LONG_TYPE_INFO } |
      "FLOAT" ^^ { ti => BasicTypeInfo.FLOAT_TYPE_INFO } |
      "DOUBLE" ^^ { ti => BasicTypeInfo.DOUBLE_TYPE_INFO } |
      ("BOOL" | "BOOLEAN" ) ^^ { ti => BasicTypeInfo.BOOLEAN_TYPE_INFO } |
      "STRING" ^^ { ti => BasicTypeInfo.STRING_TYPE_INFO } |
      "DATE" ^^ { ti => BasicTypeInfo.DATE_TYPE_INFO }

  // Literals

  lazy val numberLiteral: PackratParser[Expression] =
    ((wholeNumber <~ ("L" | "l")) | floatingPointNumber | decimalNumber | wholeNumber) ^^ {
      str =>
        if (str.endsWith("L") || str.endsWith("l")) {
          Literal(str.toLong)
        } else if (str.matches("""-?\d+""")) {
          Literal(str.toInt)
        } else if (str.endsWith("f") | str.endsWith("F")) {
          Literal(str.toFloat)
        } else {
          Literal(str.toDouble)
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
    case dt => Null(dt)
  }

  lazy val literalExpr: PackratParser[Expression] =
    numberLiteral |
      stringLiteralFlink | singleQuoteStringLiteral |
      boolLiteral | nullLiteral

  lazy val fieldReference: PackratParser[Expression] = ident ^^ {
    case sym => UnresolvedFieldReference(sym)
  }

  lazy val atom: PackratParser[Expression] =
    ( "(" ~> expression <~ ")" ) | literalExpr | fieldReference

  // suffix operators

  lazy val suffixIsNull: PackratParser[Expression] =
    composite <~ "." ~ IS_NULL ~ opt("()") ^^ { e => IsNull(e) }

  lazy val suffixIsNotNull: PackratParser[Expression] =
    composite <~ "." ~ IS_NOT_NULL ~ opt("()") ^^ { e => IsNotNull(e) }

  lazy val suffixAsc : PackratParser[Expression] =
    (atom <~ ".asc" ^^ { e => Asc(e) }) | (atom <~ ASC ^^ { e => Asc(e) })

  lazy val suffixDesc : PackratParser[Expression] =
    (atom <~ ".desc" ^^ { e => Desc(e) }) | (atom <~ DESC ^^ { e => Desc(e) })


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
    case e ~ _ ~ _ ~ _ ~ target ~ _ => Naming(e, target.name)
  }

  lazy val suffixEval: PackratParser[Expression] =
    composite ~ "." ~ EVAL ~ "(" ~ expression ~ "," ~ expression ~ ")" ^^ {
    case condition ~ _ ~ _ ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ => Eval(condition, ifTrue, ifFalse)
  }

  lazy val suffixFunctionCall =
    composite ~ "." ~ functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
    case operand ~ _ ~ name ~ _ ~ args ~ _ => Call(name.toUpperCase, operand :: args : _*)
  }

  lazy val suffixTrim = composite ~ ".trim(" ~ ("BOTH" | "LEADING" | "TRAILING") ~ "," ~
      expression ~ ")" ^^ {
    case operand ~ _ ~ trimType ~ _ ~ trimCharacter ~ _ =>
      val flag = trimType match {
        case "BOTH" => BuiltInFunctionConstants.TRIM_BOTH
        case "LEADING" => BuiltInFunctionConstants.TRIM_LEADING
        case "TRAILING" => BuiltInFunctionConstants.TRIM_TRAILING
      }
      Call(BuiltInFunctionNames.TRIM, flag, trimCharacter, operand)
  }

  lazy val suffixTrimWithoutArgs = composite <~ ".trim" ~ opt("()") ^^ {
    case e =>
      Call(
        BuiltInFunctionNames.TRIM,
        BuiltInFunctionConstants.TRIM_BOTH,
        BuiltInFunctionConstants.TRIM_DEFAULT_CHAR,
        e)
  }

  lazy val suffixed: PackratParser[Expression] =
    suffixIsNull | suffixIsNotNull | suffixSum | suffixMin | suffixMax | suffixCount | suffixAvg |
      suffixCast | suffixAs | suffixTrim | suffixTrimWithoutArgs | suffixEval | suffixFunctionCall |
        suffixAsc | suffixDesc

  // prefix operators

  lazy val prefixIsNull: PackratParser[Expression] =
    IS_NULL ~ "(" ~> expression <~ ")" ^^ { e => IsNull(e) }

  lazy val prefixIsNotNull: PackratParser[Expression] =
    IS_NOT_NULL ~ "(" ~> expression <~ ")" ^^ { e => IsNotNull(e) }

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
    case _ ~ _ ~ e ~ _ ~ target ~ _ => Naming(e, target.name)
  }

  lazy val prefixEval: PackratParser[Expression] = composite ~
      EVAL ~ "(" ~ expression ~ "," ~ expression ~ "," ~ expression ~ ")" ^^ {
    case _ ~ _ ~ condition ~ _ ~ ifTrue ~ _ ~ ifFalse ~ _ => Eval(condition, ifTrue, ifFalse)
  }

  lazy val prefixFunctionCall = functionIdent ~ "(" ~ repsep(expression, ",") ~ ")" ^^ {
    case name ~ _ ~ args ~ _ => Call(name.toUpperCase, args: _*)
  }

  lazy val prefixTrim = "trim(" ~ ("BOTH" | "LEADING" | "TRAILING") ~ "," ~ expression ~
      "," ~ expression ~ ")" ^^ {
    case _ ~ trimType ~ _ ~ trimCharacter ~ _ ~ operand ~ _ =>
      val flag = trimType match {
        case "BOTH" => BuiltInFunctionConstants.TRIM_BOTH
        case "LEADING" => BuiltInFunctionConstants.TRIM_LEADING
        case "TRAILING" => BuiltInFunctionConstants.TRIM_TRAILING
      }
      Call(BuiltInFunctionNames.TRIM, flag, trimCharacter, operand)
  }

  lazy val prefixTrimWithoutArgs = "trim(" ~ expression ~ ")" ^^ {
    case _ ~ operand ~ _ =>
      Call(
        BuiltInFunctionNames.TRIM,
        BuiltInFunctionConstants.TRIM_BOTH,
        BuiltInFunctionConstants.TRIM_DEFAULT_CHAR,
        operand)
  }

  lazy val prefixed: PackratParser[Expression] =
    prefixIsNull | prefixIsNotNull | prefixSum | prefixMin | prefixMax | prefixCount | prefixAvg |
      prefixCast | prefixAs | prefixTrim | prefixTrimWithoutArgs | prefixEval | prefixFunctionCall

  // suffix/prefix composite

  lazy val composite: PackratParser[Expression] = suffixed | prefixed | atom

  // unary ops

  lazy val unaryNot: PackratParser[Expression] = "!" ~> composite ^^ { e => Not(e) }

  lazy val unaryMinus: PackratParser[Expression] = "-" ~> composite ^^ { e => UnaryMinus(e) }

  lazy val unary = unaryNot | unaryMinus | composite

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
    case e ~ _ ~ name => Naming(e, name.name)
  } | logic

  lazy val expression: PackratParser[Expression] = alias

  lazy val expressionList: Parser[List[Expression]] = rep1sep(expression, ",")

  def parseExpressionList(expression: String): List[Expression] = {
    parseAll(expressionList, expression) match {
      case Success(lst, _) => lst

      case Failure(msg, _) => throw new ExpressionParserException(
        "Could not parse expression: " + msg)

      case Error(msg, _) => throw new ExpressionParserException(
        "Could not parse expression: " + msg)
    }
  }

  def parseExpression(exprString: String): Expression = {
    parseAll(expression, exprString) match {
      case Success(lst, _) => lst

      case fail =>
        throw new ExpressionParserException("Could not parse expression: " + fail.toString)
    }
  }
}
