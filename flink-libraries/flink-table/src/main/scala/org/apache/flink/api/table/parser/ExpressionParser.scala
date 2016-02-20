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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.ExpressionException
import org.apache.flink.api.table.expressions._

import scala.util.parsing.combinator.{PackratParsers, JavaTokenParsers}

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

  // KeyWord

  lazy val AS: Keyword = Keyword("as")
  lazy val COUNT: Keyword = Keyword("count")
  lazy val AVG: Keyword = Keyword("avg")
  lazy val MIN: Keyword = Keyword("min")
  lazy val MAX: Keyword = Keyword("max")
  lazy val SUM: Keyword = Keyword("sum")

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

  lazy val sum: PackratParser[Expression] =
    (atom <~ ".sum" ^^ { e => Sum(e) }) | (SUM ~ "(" ~> atom <~ ")" ^^ { e => Sum(e) })
  lazy val min: PackratParser[Expression] =
    (atom <~ ".min" ^^ { e => Min(e) }) | (MIN ~ "(" ~> atom <~ ")" ^^ { e => Min(e) })
  lazy val max: PackratParser[Expression] =
    (atom <~ ".max" ^^ { e => Max(e) }) | (MAX ~ "(" ~> atom <~ ")" ^^ { e => Max(e) })
  lazy val count: PackratParser[Expression] =
    (atom <~ ".count" ^^ { e => Count(e) }) | (COUNT ~ "(" ~> atom <~ ")" ^^ { e => Count(e) })
  lazy val avg: PackratParser[Expression] =
    (atom <~ ".avg" ^^ { e => Avg(e) }) | (AVG ~ "(" ~> atom <~ ")" ^^ { e => Avg(e) })

  lazy val cast: PackratParser[Expression] =
    atom <~ ".cast(BYTE)" ^^ { e => Cast(e, BasicTypeInfo.BYTE_TYPE_INFO) } |
    atom <~ ".cast(SHORT)" ^^ { e => Cast(e, BasicTypeInfo.SHORT_TYPE_INFO) } |
    atom <~ ".cast(INT)" ^^ { e => Cast(e, BasicTypeInfo.INT_TYPE_INFO) } |
    atom <~ ".cast(LONG)" ^^ { e => Cast(e, BasicTypeInfo.LONG_TYPE_INFO) } |
    atom <~ ".cast(FLOAT)" ^^ { e => Cast(e, BasicTypeInfo.FLOAT_TYPE_INFO) } |
    atom <~ ".cast(DOUBLE)" ^^ { e => Cast(e, BasicTypeInfo.DOUBLE_TYPE_INFO) } |
    atom <~ ".cast(BOOL)" ^^ { e => Cast(e, BasicTypeInfo.BOOLEAN_TYPE_INFO) } |
    atom <~ ".cast(BOOLEAN)" ^^ { e => Cast(e, BasicTypeInfo.BOOLEAN_TYPE_INFO) } |
    atom <~ ".cast(STRING)" ^^ { e => Cast(e, BasicTypeInfo.STRING_TYPE_INFO) } |
    atom <~ ".cast(DATE)" ^^ { e => Cast(e, BasicTypeInfo.DATE_TYPE_INFO) }

  lazy val as: PackratParser[Expression] = atom ~ ".as(" ~ fieldReference ~ ")" ^^ {
    case e ~ _ ~ as ~ _ => Naming(e, as.name)
  }

  lazy val substring: PackratParser[Expression] =
    atom ~ ".substring(" ~ expression ~ "," ~ expression ~ ")" ^^ {
      case e ~ _ ~ from ~ _ ~ to ~ _ => Substring(e, from, Some(to))

    }

  lazy val substringWithoutEnd: PackratParser[Expression] =
    atom ~ ".substring(" ~ expression ~ ")" ^^ {
      case e ~ _ ~ from ~ _ => Substring(e, from)

    }

  lazy val suffix =
    isNull | isNotNull |
      abs | sum | min | max | count | avg | cast |
      substring | substringWithoutEnd | atom


  // unary ops

  lazy val unaryNot: PackratParser[Expression] = "!" ~> suffix ^^ { e => Not(e) }

  lazy val unaryMinus: PackratParser[Expression] = "-" ~> suffix ^^ { e => UnaryMinus(e) }

  lazy val unary = unaryNot | unaryMinus | suffix

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
