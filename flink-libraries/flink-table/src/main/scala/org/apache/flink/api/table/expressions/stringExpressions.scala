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
      ValidationFailure(s"CharLength only accepts String input, get ${child.resultType}")
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
      ValidationFailure(s"InitCap only accepts String input, get ${child.resultType}")
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
      ValidationFailure(s"Like only accepts (String, String) input, " +
        s"get (${str.resultType}, ${pattern.resultType})")
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
      ValidationFailure(s"Lower only accepts String input, get ${child.resultType}")
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
      ValidationFailure(s"Similar only accepts (String, String) input, " +
        s"get (${str.resultType}, ${pattern.resultType})")
    }
  }

  override def toString: String = s"($str).similarTo($pattern)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SIMILAR_TO, children.map(_.toRexNode))
  }
}

/**
  * Returns subString of `str` from `begin`(inclusive) to `end`(not inclusive).
  */
case class SubString(
    str: Expression,
    begin: Expression,
    end: Expression) extends Expression with InputTypeSpec {

  def this(str: Expression, begin: Expression) = this(str, begin, CharLength(str))

  override private[flink] def children: Seq[Expression] = str :: begin :: end :: Nil

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  override def toString: String = s"$str.subString($begin, $end)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
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
    str: Expression) extends Expression with InputTypeSpec {

  override private[flink] def children: Seq[Expression] = trimFlag :: trimString :: str :: Nil

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(INT_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO)

  override def toString: String = s"trim($trimFlag, $trimString, $str)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
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
  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Upper only accepts String input, get ${child.resultType}")
    }
  }

  override def toString: String = s"($child).toUpperCase()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.UPPER, child.toRexNode)
  }
}
