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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.expressions.TrimMode.TrimMode
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.validate._

import scala.collection.JavaConversions._

/**
  * Returns the length of this `str`.
  */
case class CharLength(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.INT

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == DataTypes.STRING) {
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

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns str with the first letter of each word in uppercase.
  * All other letters are in lowercase. Words are delimited by white space.
  */
case class InitCap(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == DataTypes.STRING) {
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

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns true if `str` matches `pattern`.
  */
case class Like(str: Expression, pattern: Expression) extends BinaryExpression {
  private[flink] def left: Expression = str
  private[flink] def right: Expression = pattern

  override private[flink] def resultType: InternalType = DataTypes.BOOLEAN

  override private[flink] def validateInput(): ValidationResult = {
    if (str.resultType == DataTypes.STRING && pattern.resultType == DataTypes.STRING) {
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

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns str with all characters changed to lowercase.
  */
case class Lower(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == DataTypes.STRING) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Lower operator requires String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).lowerCase()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LOWER, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns true if `str` is similar to `pattern`.
  */
case class Similar(str: Expression, pattern: Expression) extends BinaryExpression {
  private[flink] def left: Expression = str
  private[flink] def right: Expression = pattern

  override private[flink] def resultType: InternalType = DataTypes.BOOLEAN

  override private[flink] def validateInput(): ValidationResult = {
    if (str.resultType == DataTypes.STRING && pattern.resultType == DataTypes.STRING) {
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

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
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

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.INT, DataTypes.INT)

  override def toString: String = s"($str).substring($begin, $length)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.SUBSTRING, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns the leftmost n characters of `str`.
  */
case class Left(
    str: Expression,
    length: Expression) extends Expression with InputTypeSpec {

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.INT)

  override private[flink] def children: Seq[Expression] = str :: length :: Nil

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)

  override def toString: String = s"($str).left($length)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.LEFT, children.map(_.toRexNode))
  }
}

/**
  * Returns the rightmost n characters of `str`.
  */
case class Right(
    str: Expression,
    length: Expression) extends Expression with InputTypeSpec {

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.INT)

  override private[flink] def children: Seq[Expression] = str :: length :: Nil

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)

  override def toString: String = s"($str).right($length)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.RIGHT, children.map(_.toRexNode))
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

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def validateInput(): ValidationResult = {
    trimMode match {
      case SymbolExpression(_: TrimMode) =>
        if (trimString.resultType != DataTypes.STRING) {
          ValidationFailure(s"String expected for trimString, get ${trimString.resultType}")
        } else if (str.resultType != DataTypes.STRING) {
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

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Trim `trimString` from `str` starting from left side.
  */
case class Ltrim(
    str: Expression,
    trimString: Expression) extends Expression {

  def this(str: Expression) = this(str, TrimConstants.TRIM_DEFAULT_CHAR)

  override private[flink] def children: Seq[Expression] = str :: trimString :: Nil

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override def toString: String = s"($str).ltrim($trimString)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.LTRIM, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Trim `trimString` from `str` starting from right side.
  */
case class Rtrim(
    str: Expression,
    trimString: Expression) extends Expression {

  def this(str: Expression) = this(str, TrimConstants.TRIM_DEFAULT_CHAR)

  override private[flink] def children: Seq[Expression] = str :: trimString :: Nil

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override def toString: String = s"($str).rtrim($trimString)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.RTRIM, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
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

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING)

  override def toString: String = s"($child).upperCase()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.UPPER, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns the position of string needle in string haystack.
  */
case class Position(needle: Expression, haystack: Expression)
    extends Expression with InputTypeSpec {

  override private[flink] def children: Seq[Expression] = Seq(needle, haystack)

  override private[flink] def resultType: InternalType = DataTypes.INT

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.STRING)

  override def toString: String = s"($needle).position($haystack)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POSITION, needle.toRexNode, haystack.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns the position of string needle in string haystack.
  */
case class Instr(
    str: Expression,
    subString: Expression,
    startPosition: Expression,
    nthAppearance: Expression)
    extends Expression with InputTypeSpec {

  def this(str: Expression, subString: Expression) =
    this(str, subString, Literal(1), Literal(1))

  def this(str: Expression, subString: Expression, startPosition: Expression) =
    this(str, subString, startPosition, Literal(1))

  override private[flink] def children: Seq[Expression] =
    Seq(str, subString, startPosition, nthAppearance)

  override private[flink] def resultType: InternalType = DataTypes.INT

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.INT, DataTypes.INT)

  override def toString: String = s"($str).instr($subString, $startPosition, $nthAppearance)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(
      ScalarSqlFunctions.INSTR,
      str.toRexNode,
      subString.toRexNode,
      startPosition.toRexNode,
      nthAppearance.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
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

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.INT, DataTypes.INT)

  override def toString: String = s"($str).overlay($replacement, $starting, $position)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(
      SqlStdOperatorTable.OVERLAY,
      str.toRexNode,
      replacement.toRexNode,
      starting.toRexNode,
      position.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Locate(needle: Expression, haystack: Expression, starting: Expression)
    extends Expression with InputTypeSpec {

  def this(needle: Expression, haystack: Expression) =
    this(needle, haystack, Literal(1))

  override private[flink] def children: Seq[Expression] = Seq(needle, haystack, starting)

  override private[flink] def resultType: InternalType = DataTypes.INT

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.INT)

  override def toString: String = s"($needle).locate($haystack, $starting)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(
      ScalarSqlFunctions.LOCATE,
      needle.toRexNode,
      haystack.toRexNode,
      starting.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns the string that results from concatenating the arguments.
  * Returns NULL if any argument is NULL.
  */
case class Concat(strings: Seq[Expression]) extends Expression with InputTypeSpec {

  override private[flink] def children: Seq[Expression] = strings

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    children.map(_ => DataTypes.STRING)

  override def toString: String = s"concat($strings)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.CONCAT, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns the string that results from concatenating the arguments and separator.
  * Returns NULL If the separator is NULL.
  *
  * Note: this user-defined function does not skip empty strings. However, it does skip any NULL
  * values after the separator argument.
  **/
case class ConcatWs(separator: Expression, strings: Seq[Expression])
  extends Expression with InputTypeSpec {

  override private[flink] def children: Seq[Expression] = Seq(separator) ++ strings

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    children.map(_ => DataTypes.STRING)

  override def toString: String = s"concat_ws($separator, $strings)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.CONCAT_WS, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Gets the ascii code (0~255) of the first character in a string.
  * Returns 0 if the string is empty. Returns null if the string is Null.
  * Returns the ascii of the first byte if the first character cannot be holded in
  * one byte (exceed 255).
  **/
case class Ascii(str: Expression) extends Expression with InputTypeSpec {

  override private[flink] def resultType: InternalType = DataTypes.INT

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING)

  override private[flink] def children: Seq[Expression] = str :: Nil

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)

  override def toString: String = s"($str).ascii()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.ASCII, children.map(_.toRexNode))
  }
}

case class Encode(
    str: Expression,
    charset: Expression) extends Expression with InputTypeSpec {

  override private[flink] def resultType: InternalType = DataTypes.BYTE

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.STRING)

  override private[flink] def children: Seq[Expression] = str :: charset :: Nil

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)

  override def toString: String = s"($str).encode($charset)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.ENCODE, children.map(_.toRexNode))
  }
}

case class Decode(
    binary: Expression,
    charset: Expression) extends Expression with InputTypeSpec {

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.BYTE, DataTypes.STRING)

  override private[flink] def children: Seq[Expression] = binary :: charset :: Nil

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)

  override def toString: String = s"($binary).decode($charset)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.DECODE, children.map(_.toRexNode))
  }
}

case class Lpad(text: Expression, len: Expression, pad: Expression)
  extends Expression with InputTypeSpec {

  override private[flink] def children: Seq[Expression] = Seq(text, len, pad)

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.INT, DataTypes.STRING)

  override def toString: String = s"($text).lpad($len, $pad)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.LPAD, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Rpad(text: Expression, len: Expression, pad: Expression)
  extends Expression with InputTypeSpec {

  override private[flink] def children: Seq[Expression] = Seq(text, len, pad)

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.INT, DataTypes.STRING)

  override def toString: String = s"($text).rpad($len, $pad)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.RPAD, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns a string with all substrings that match the regular expression consecutively
  * being replaced.
  */
case class RegexpReplace(str: Expression, regex: Expression, replacement: Expression)
    extends Expression with InputTypeSpec {

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.STRING)

  override private[flink] def children: Seq[Expression] = Seq(str, regex, replacement)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.REGEXP_REPLACE, children.map(_.toRexNode))
  }

  override def toString: String = s"($str).regexp_replace($regex, $replacement)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

  /**
  * Returns a string extracted with a specified regular expression and a regex match group index.
  */
case class RegexpExtract(str: Expression, regex: Expression, extractIndex: Expression)
    extends Expression with InputTypeSpec {
  def this(str: Expression, regex: Expression) = this(str, regex, null)

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] = {
    if (extractIndex == null) {
      Seq(DataTypes.STRING, DataTypes.STRING)
    } else {
      Seq(
        DataTypes.STRING,
        DataTypes.STRING,
        DataTypes.INT)
    }
  }

  override private[flink] def children: Seq[Expression] = {
    if (extractIndex == null) {
      Seq(str, regex)
    } else {
      Seq(str, regex, extractIndex)
    }
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.REGEXP_EXTRACT, children.map(_.toRexNode))
  }

  override def toString: String = s"($str).regexp_extract($regex, $extractIndex)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

object RegexpExtract {
  def apply(str: Expression, regex: Expression): RegexpExtract = RegexpExtract(str, regex, null)
}

/**
  * Returns a string that repeats the base str n times.
  */
case class Repeat(str: Expression, n: Expression) extends Expression with InputTypeSpec {

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.INT)

  override private[flink] def children: Seq[Expression] = Seq(str, n)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.REPEAT, str.toRexNode, n.toRexNode)
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (str.resultType == DataTypes.STRING && n.resultType == DataTypes.INT) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Repeat operator requires (String, Int) input, " +
          s"but ($str, $n) is of type (${str.resultType}, ${n.resultType})")
    }
  }

  override def toString: String = s"($str).repeat($n)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
* Returns a new string which replaces all the occurrences of the search target
* with the replacement string (non-overlapping).
*/
case class Replace(str: Expression,
    search: Expression,
    replacement: Expression) extends Expression with InputTypeSpec {

  def this(str: Expression, begin: Expression) = this(str, begin, CharLength(str))

  override private[flink] def children: Seq[Expression] = str :: search :: replacement :: Nil

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)

  override def toString: String = s"($str).replace($search, $replacement)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.REPLACE, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class UUID() extends LeafExpression {
  override private[flink] def resultType = DataTypes.STRING

  override def toString: String = s"uuid()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.UUID)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns the base byte array decoded with base64.
  * Returns NULL If the input string is NULL.
  */
case class FromBase64(child: Expression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def expectedTypes: Seq[InternalType] = Seq(DataTypes.STRING)

  override private[flink] def resultType: InternalType = DataTypes.BYTE_ARRAY

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == DataTypes.STRING) {
      ValidationSuccess
    } else {
      ValidationFailure(s"FromBase64 operator requires String input, " +
          s"but $child is of type ${child.resultType}")
    }
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.FROM_BASE64, child.toRexNode)
  }

  override def toString: String = s"($child).fromBase64"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns the base64-encoded result of the input byte array.
  */
case class ToBase64(child: Expression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def expectedTypes: Seq[InternalType] = Seq(DataTypes.BYTE_ARRAY)

  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == DataTypes.BYTE_ARRAY) {
      ValidationSuccess
    } else {
      ValidationFailure(s"ToBase64 operator requires a ByteArray input, " +
          s"but $child is of type ${child.resultType}")
    }
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.TO_BASE64, child.toRexNode)
  }

  override def toString: String = s"($child).toBase64"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
