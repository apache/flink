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
package org.apache.flink.api.table.parser

import org.apache.flink.api.table.ExpressionException
import org.apache.flink.api.table.plan.As
import org.apache.flink.api.table.expressions._

import scala.util.parsing.combinator.{PackratParsers, JavaTokenParsers}

/**
 * Parser for expressions inside a String. This parses exactly the same expressions that
 * would be accepted by the Scala Expression DSL.
 *
 * See [[org.apache.flink.api.scala.expressions.ImplicitExpressionConversions]] and
 * [[org.apache.flink.api.scala.expressions.ImplicitExpressionOperations]] for the constructs
 * available in the Scala Expression DSL. This parser must be kept in sync with the Scala DSL
 * lazy valined in the above files.
 */
object ExpressionParser extends JavaTokenParsers with PackratParsers {

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

  lazy val literalExpr: PackratParser[Expression] =
    numberLiteral |
      stringLiteralFlink | singleQuoteStringLiteral |
      boolLiteral

  lazy val fieldReference: PackratParser[Expression] = ident ^^ {
    case sym => UnresolvedFieldReference(sym)
  }

  lazy val atom: PackratParser[Expression] =
    ( "(" ~> expression <~ ")" ) | literalExpr | fieldReference

  // suffix ops
  lazy val isNull: PackratParser[Expression] = atom <~ ".isNull" ^^ { e => IsNull(e) }
  lazy val isNotNull: PackratParser[Expression] = atom <~ ".isNotNull" ^^ { e => IsNotNull(e) }

  lazy val abs: PackratParser[Expression] = atom <~ ".abs" ^^ { e => Abs(e) }

  lazy val sum: PackratParser[Expression] = atom <~ ".sum" ^^ { e => Sum(e) }
  lazy val min: PackratParser[Expression] = atom <~ ".min" ^^ { e => Min(e) }
  lazy val max: PackratParser[Expression] = atom <~ ".max" ^^ { e => Max(e) }
  lazy val count: PackratParser[Expression] = atom <~ ".count" ^^ { e => Count(e) }
  lazy val avg: PackratParser[Expression] = atom <~ ".avg" ^^ { e => Avg(e) }

  lazy val as: PackratParser[Expression] = atom ~ ".as(" ~ fieldReference ~ ")" ^^ {
    case e ~ _ ~ as ~ _ => Naming(e, as.name)
  }

  lazy val substring: PackratParser[Expression] =
    atom ~ ".substring(" ~ expression ~ "," ~ expression ~ ")" ^^ {
      case e ~ _ ~ from ~ _ ~ to ~ _ => Substring(e, from, to)

    }

  lazy val substringWithoutEnd: PackratParser[Expression] =
    atom ~ ".substring(" ~ expression ~ ")" ^^ {
      case e ~ _ ~ from ~ _ => Substring(e, from, Literal(Integer.MAX_VALUE))

    }

  lazy val suffix =
    isNull | isNotNull |
      abs | sum | min | max | count | avg |
      substring | substringWithoutEnd | atom


  // unary ops

  lazy val unaryNot: PackratParser[Expression] = "!" ~> suffix ^^ { e => Not(e) }

  lazy val unaryMinus: PackratParser[Expression] = "-" ~> suffix ^^ { e => UnaryMinus(e) }

  lazy val unaryBitwiseNot: PackratParser[Expression] = "~" ~> suffix ^^ { e => BitwiseNot(e) }

  lazy val unary = unaryNot | unaryMinus | unaryBitwiseNot | suffix

  // binary bitwise opts

  lazy val binaryBitwise = unary * (
    "&" ^^^ { (a:Expression, b:Expression) => BitwiseAnd(a,b) } |
      "|" ^^^ { (a:Expression, b:Expression) => BitwiseOr(a,b) } |
      "^" ^^^ { (a:Expression, b:Expression) => BitwiseXor(a,b) } )

  // arithmetic

  lazy val product = binaryBitwise * (
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

  lazy val alias: PackratParser[Expression] = logic ~ "as" ~ fieldReference ^^ {
    case e ~ _ ~ name => Naming(e, name.name)
  } | logic

  lazy val expression: PackratParser[Expression] = alias

  lazy val expressionList: Parser[List[Expression]] = rep1sep(expression, ",")

  def parseExpressionList(expression: String): List[Expression] = {
    parseAll(expressionList, expression) match {
      case Success(lst, _) => lst

      case Failure(msg, _) => throw new ExpressionException("Could not parse expression: " + msg)

      case Error(msg, _) => throw new ExpressionException("Could not parse expression: " + msg)
    }
  }

  def parseExpression(exprString: String): Expression = {
    parseAll(expression, exprString) match {
      case Success(lst, _) => lst

      case fail =>
        throw new ExpressionException("Could not parse expression: " + fail.toString)
    }
  }
}
