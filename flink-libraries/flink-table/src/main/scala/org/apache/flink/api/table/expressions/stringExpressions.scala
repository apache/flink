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
import org.apache.flink.api.table.validate.ExprValidationResult

/**
  * Returns the length of this `str`.
  */
case class CharLength(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = INT_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (child.dataType == STRING_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"CharLength only accept String input, get ${child.dataType}")
    }
  }

  override def toString(): String = s"($child).charLength()"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.CHAR_LENGTH, child.toRexNode)
  }
}

/**
  * Returns str with the first letter of each word in uppercase.
  * All other letters are in lowercase. Words are delimited by white space.
  */
case class InitCap(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = STRING_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (child.dataType == STRING_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"InitCap only accept String input, get ${child.dataType}")
    }
  }

  override def toString(): String = s"($child).initCap()"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.INITCAP, child.toRexNode)
  }
}

/**
  * Returns true if `str` matches `pattern`.
  */
case class Like(str: Expression, pattern: Expression) extends BinaryExpression {
  def left: Expression = str
  def right: Expression = pattern

  override def dataType: TypeInformation[_] = BOOLEAN_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (str.dataType == STRING_TYPE_INFO && pattern.dataType == STRING_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"Like only accept (String, String) input, get (${str.dataType}, ${pattern.dataType})")
    }
  }

  override def toString(): String = s"($str).like($pattern)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LIKE, children.map(_.toRexNode))
  }
}

/**
  * Returns str with all characters changed to lowercase.
  */
case class Lower(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = STRING_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (child.dataType == STRING_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"Lower only accept String input, get ${child.dataType}")
    }
  }

  override def toString(): String = s"($child).toLowerCase()"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LOWER, child.toRexNode)
  }
}

/**
  * Returns true if `str` is similar to `pattern`.
  */
case class Similar(str: Expression, pattern: Expression) extends BinaryExpression {
  def left: Expression = str
  def right: Expression = pattern

  override def dataType: TypeInformation[_] = BOOLEAN_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (str.dataType == STRING_TYPE_INFO && pattern.dataType == STRING_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"Similar only accept (String, String) input, get (${str.dataType}, ${pattern.dataType})")
    }
  }

  override def toString(): String = s"($str).similarTo($pattern)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SIMILAR_TO, children.map(_.toRexNode))
  }
}

/**
  * Returns subString of `str` from `begin`(inclusive) to `end`(not inclusive).
  */
case class SubString(str: Expression, begin: Expression, end: Expression) extends Expression {

  def this(str: Expression, begin: Expression) = this(str, begin, CharLength(str))

  override def children: Seq[Expression] = str :: begin :: end :: Nil

  override def dataType: TypeInformation[_] = STRING_TYPE_INFO

  // TODO: this could be loosened by enabling implicit cast
  override def validateInput(): ExprValidationResult = {
    if (str.dataType == STRING_TYPE_INFO &&
        begin.dataType == INT_TYPE_INFO &&
        end.dataType == INT_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        "subString only accept (String, Int, Int) input, " +
          s"get (${str.dataType}, ${begin.dataType}, ${end.dataType})")
    }
  }

  override def toString(): String = s"$str.subString($begin, $end)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SUBSTRING, children.map(_.toRexNode))
  }
}

/**
  * Trim `trimString` from `str` according to `trimFlag`:
  * 0 for TRIM_BOTH, 1 for TRIM_LEADING and 2 for TRIM_TRAILING.
  */
case class Trim(
    trimFlag: Expression,
    trimString: Expression,
    str: Expression) extends Expression {

  override def children: Seq[Expression] = trimFlag :: trimString :: str :: Nil

  override def dataType: TypeInformation[_] = STRING_TYPE_INFO

  // TODO: this could be loosened by enabling implicit cast
  override def validateInput(): ExprValidationResult = {
    if (trimFlag.dataType == INT_TYPE_INFO &&
      trimString.dataType == STRING_TYPE_INFO &&
      str.dataType == STRING_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        "subString only accept (Int, String, String) input, " +
          s"get (${trimFlag.dataType}, ${trimString.dataType}, ${str.dataType})")
    }
  }

  override def toString(): String = s"trim($trimFlag, $trimString, $str)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.TRIM, children.map(_.toRexNode))
  }
}

/**
  * Enumeration of trim flags.
  */
object TrimConstants {
  val TRIM_BOTH = Literal(0)
  val TRIM_LEADING = Literal(1)
  val TRIM_TRAILING = Literal(2)
  val TRIM_DEFAULT_CHAR = Literal(" ")
}

/**
  * Returns str with all characters changed to uppercase.
  */
case class Upper(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = STRING_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (child.dataType == STRING_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"Upper only accept String input, get ${child.dataType}")
    }
  }

  override def toString(): String = s"($child).toUpperCase()"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.UPPER, child.toRexNode)
  }
}
