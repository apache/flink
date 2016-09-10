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

import scala.collection.JavaConversions._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.TrimMode.TrimMode
import org.apache.flink.api.table.validate._

/**
  * Returns the length of this `str`.
  */
case class CharLength(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = INT_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"CharLength operator requires String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).charLength()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.CHAR_LENGTH, child.toRexNode)
  }
}

/**
  * Returns str with the first letter of each word in uppercase.
  * All other letters are in lowercase. Words are delimited by white space.
  */
case class InitCap(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"InitCap operator requires String input, " + 
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).initCap()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.INITCAP, child.toRexNode)
  }
}

/**
  * Returns true if `str` matches `pattern`.
  */
case class Like(str: Expression, pattern: Expression) extends BinaryExpression {
  private[flink] def left: Expression = str
  private[flink] def right: Expression = pattern

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    if (str.resultType == STRING_TYPE_INFO && pattern.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Like operator requires (String, String) input, " +
        s"but ($str, $pattern) is of type (${str.resultType}, ${pattern.resultType})")
    }
  }

  override def toString: String = s"($str).like($pattern)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LIKE, children.map(_.toRexNode))
  }
}

/**
  * Returns str with all characters changed to lowercase.
  */
case class Lower(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Lower operator requires String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).toLowerCase()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LOWER, child.toRexNode)
  }
}

/**
  * Returns true if `str` is similar to `pattern`.
  */
case class Similar(str: Expression, pattern: Expression) extends BinaryExpression {
  private[flink] def left: Expression = str
  private[flink] def right: Expression = pattern

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    if (str.resultType == STRING_TYPE_INFO && pattern.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Similar operator requires (String, String) input, " +
        s"but ($str, $pattern) is of type (${str.resultType}, ${pattern.resultType})")
    }
  }

  override def toString: String = s"($str).similarTo($pattern)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SIMILAR_TO, children.map(_.toRexNode))
  }
}

/**
  * Returns substring of `str` from `begin`(inclusive) for `length`.
  */
case class Substring(
    str: Expression,
    begin: Expression,
    length: Expression) extends Expression with InputTypeSpec {

  def this(str: Expression, begin: Expression) = this(str, begin, CharLength(str))

  override private[flink] def children: Seq[Expression] = str :: begin :: length :: Nil

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  override def toString: String = s"($str).substring($begin, $length)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SUBSTRING, children.map(_.toRexNode))
  }
}

/**
  * Trim `trimString` from `str` according to `trimMode`.
  */
case class Trim(
    trimMode: Expression,
    trimString: Expression,
    str: Expression) extends Expression {

  override private[flink] def children: Seq[Expression] = trimMode :: trimString :: str :: Nil

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    trimMode match {
      case SymbolExpression(_: TrimMode) =>
        if (trimString.resultType != STRING_TYPE_INFO) {
          ValidationFailure(s"String expected for trimString, get ${trimString.resultType}")
        } else if (str.resultType != STRING_TYPE_INFO) {
          ValidationFailure(s"String expected for str, get ${str.resultType}")
        } else {
          ValidationSuccess
        }
      case _ => ValidationFailure("TrimMode symbol expected.")
    }
  }

  override def toString: String = s"($str).trim($trimMode, $trimString)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.TRIM, children.map(_.toRexNode))
  }
}

/**
  * Enumeration of trim flags.
  */
object TrimConstants {
  val TRIM_DEFAULT_CHAR = Literal(" ")
}

/**
  * Returns str with all characters changed to uppercase.
  */
case class Upper(child: Expression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO)

  override def toString: String = s"($child).upperCase()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.UPPER, child.toRexNode)
  }
}

/**
  * Returns the position of string needle in string haystack.
  */
case class Position(needle: Expression, haystack: Expression)
    extends Expression with InputTypeSpec {

  override private[flink] def children: Seq[Expression] = Seq(needle, haystack)

  override private[flink] def resultType: TypeInformation[_] = INT_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO)

  override def toString: String = s"($needle).position($haystack)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POSITION, needle.toRexNode, haystack.toRexNode)
  }
}

/**
  * Replaces a substring of a string with a replacement string.
  * Starting at a position for a given length.
  */
case class Overlay(
    str: Expression,
    replacement: Expression,
    starting: Expression,
    position: Expression)
  extends Expression with InputTypeSpec {

  def this(str: Expression, replacement: Expression, starting: Expression) =
    this(str, replacement, starting, CharLength(replacement))

  override private[flink] def children: Seq[Expression] =
    Seq(str, replacement, starting, position)

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  override def toString: String = s"($str).overlay($replacement, $starting, $position)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(
      SqlStdOperatorTable.OVERLAY,
      str.toRexNode,
      replacement.toRexNode,
      starting.toRexNode,
      position.toRexNode)
  }
}
