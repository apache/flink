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
package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.planner.expressions.PlannerTrimMode.PlannerTrimMode
import org.apache.flink.table.planner.validate._

/**
  * Returns the length of this `str`.
  */
case class CharLength(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = INT_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"CharLength operator requires String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).charLength()"
}

/**
  * Returns str with the first letter of each word in uppercase.
  * All other letters are in lowercase. Words are delimited by white space.
  */
case class InitCap(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"InitCap operator requires String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).initCap()"
}

/**
  * Returns true if `str` matches `pattern`.
  */
case class Like(str: PlannerExpression, pattern: PlannerExpression) extends BinaryExpression {
  private[flink] def left: PlannerExpression = str
  private[flink] def right: PlannerExpression = pattern

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (str.resultType == STRING_TYPE_INFO && pattern.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Like operator requires (String, String) input, " +
        s"but ($str, $pattern) is of type (${str.resultType}, ${pattern.resultType})")
    }
  }

  override def toString: String = s"($str).like($pattern)"
}

/**
  * Returns str with all characters changed to lowercase.
  */
case class Lower(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Lower operator requires String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).lowerCase()"
}

/**
  * Returns true if `str` is similar to `pattern`.
  */
case class Similar(str: PlannerExpression, pattern: PlannerExpression) extends BinaryExpression {
  private[flink] def left: PlannerExpression = str
  private[flink] def right: PlannerExpression = pattern

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (str.resultType == STRING_TYPE_INFO && pattern.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Similar operator requires (String, String) input, " +
        s"but ($str, $pattern) is of type (${str.resultType}, ${pattern.resultType})")
    }
  }

  override def toString: String = s"($str).similarTo($pattern)"
}

/**
  * Returns substring of `str` from `begin`(inclusive) for `length`.
  */
case class Substring(
    str: PlannerExpression,
    begin: PlannerExpression,
    length: PlannerExpression) extends PlannerExpression with InputTypeSpec {

  def this(str: PlannerExpression, begin: PlannerExpression) = this(str, begin, CharLength(str))

  override private[flink] def children: Seq[PlannerExpression] = str :: begin :: length :: Nil

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  override def toString: String = s"($str).substring($begin, $length)"
}

/**
  * Trim `trimString` from `str` according to `trimMode`.
  */
case class Trim(
    trimMode: PlannerExpression,
    trimString: PlannerExpression,
    str: PlannerExpression) extends PlannerExpression {

  override private[flink] def children: Seq[PlannerExpression] =
    trimMode :: trimString :: str :: Nil

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    trimMode match {
      case SymbolPlannerExpression(_: PlannerTrimMode) =>
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
case class Upper(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO)

  override def toString: String = s"($child).upperCase()"
}

/**
  * Returns the position of string needle in string haystack.
  */
case class Position(needle: PlannerExpression, haystack: PlannerExpression)
    extends PlannerExpression with InputTypeSpec {

  override private[flink] def children: Seq[PlannerExpression] = Seq(needle, haystack)

  override private[flink] def resultType: TypeInformation[_] = INT_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO)

  override def toString: String = s"($needle).position($haystack)"
}

/**
  * Replaces a substring of a string with a replacement string.
  * Starting at a position for a given length.
  */
case class Overlay(
    str: PlannerExpression,
    replacement: PlannerExpression,
    starting: PlannerExpression,
    position: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {

  def this(str: PlannerExpression, replacement: PlannerExpression, starting: PlannerExpression) =
    this(str, replacement, starting, CharLength(replacement))

  override private[flink] def children: Seq[PlannerExpression] =
    Seq(str, replacement, starting, position)

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  override def toString: String = s"($str).overlay($replacement, $starting, $position)"
}

/**
  * Returns the string that results from concatenating the arguments.
  * Returns NULL if any argument is NULL.
  */
case class Concat(strings: Seq[PlannerExpression]) extends PlannerExpression with InputTypeSpec {

  override private[flink] def children: Seq[PlannerExpression] = strings

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    children.map(_ => STRING_TYPE_INFO)

  override def toString: String = s"concat($strings)"

}

/**
  * Returns the string that results from concatenating the arguments and separator.
  * Returns NULL If the separator is NULL.
  *
  * Note: this user-defined function does not skip empty strings. However, it does skip any NULL
  * values after the separator argument.
  **/
case class ConcatWs(separator: PlannerExpression, strings: Seq[PlannerExpression])
  extends PlannerExpression with InputTypeSpec {

  override private[flink] def children: Seq[PlannerExpression] = Seq(separator) ++ strings

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    children.map(_ => STRING_TYPE_INFO)

  override def toString: String = s"concat_ws($separator, $strings)"
}

case class Lpad(text: PlannerExpression, len: PlannerExpression, pad: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {

  override private[flink] def children: Seq[PlannerExpression] = Seq(text, len, pad)

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  override def toString: String = s"($text).lpad($len, $pad)"
}

case class Rpad(text: PlannerExpression, len: PlannerExpression, pad: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {

  override private[flink] def children: Seq[PlannerExpression] = Seq(text, len, pad)

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  override def toString: String = s"($text).rpad($len, $pad)"

}

/**
  * Returns a string with all substrings that match the regular expression consecutively
  * being replaced.
  */
case class RegexpReplace(
    str: PlannerExpression,
    regex: PlannerExpression,
    replacement: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

  override private[flink] def children: Seq[PlannerExpression] = Seq(str, regex, replacement)

  override def toString: String = s"($str).regexp_replace($regex, $replacement)"
}

/**
  * Returns a string extracted with a specified regular expression and a regex match group index.
  */
case class RegexpExtract(
    str: PlannerExpression,
    regex: PlannerExpression,
    extractIndex: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {
  def this(str: PlannerExpression, regex: PlannerExpression) = this(str, regex, null)

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = {
    if (extractIndex == null) {
      Seq(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    } else {
      Seq(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO)
    }
  }

  override private[flink] def children: Seq[PlannerExpression] = {
    if (extractIndex == null) {
      Seq(str, regex)
    } else {
      Seq(str, regex, extractIndex)
    }
  }

  override def toString: String = s"($str).regexp_extract($regex, $extractIndex)"
}

object RegexpExtract {
  def apply(str: PlannerExpression, regex: PlannerExpression): RegexpExtract =
    RegexpExtract(str, regex, null)
}

/**
  * Returns the base string decoded with base64.
  * Returns NULL If the input string is NULL.
  */
case class FromBase64(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = Seq(STRING_TYPE_INFO)

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"FromBase64 operator requires String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).fromBase64"

}

/**
  * Returns the base64-encoded result of the input string.
  */
case class ToBase64(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = Seq(STRING_TYPE_INFO)

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"ToBase64 operator requires a String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"($child).toBase64"

}

/**
  * Returns a string that removes the left whitespaces from the given string.
  */
case class LTrim(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = Seq(STRING_TYPE_INFO)

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"LTrim operator requires a String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString = s"($child).ltrim"
}

/**
  * Returns a string that removes the right whitespaces from the given string.
  */
case class RTrim(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = Seq(STRING_TYPE_INFO)

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == STRING_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"RTrim operator requires a String input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def toString = s"($child).rtrim"
}

/**
  * Returns a string that repeats the base str n times.
  */
case class Repeat(str: PlannerExpression, n: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO)

  override private[flink] def children: Seq[PlannerExpression] = Seq(str, n)

  override private[flink] def validateInput(): ValidationResult = {
    if (str.resultType == STRING_TYPE_INFO && n.resultType == INT_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Repeat operator requires (String, Int) input, " +
        s"but ($str, $n) is of type (${str.resultType}, ${n.resultType})")
    }
  }

  override def toString: String = s"($str).repeat($n)"
}

/**
  * Returns a new string which replaces all the occurrences of the search target
  * with the replacement string (non-overlapping).
  */
case class Replace(
    str: PlannerExpression,
    search: PlannerExpression,
    replacement: PlannerExpression) extends PlannerExpression with InputTypeSpec {

  override private[flink] def children: Seq[PlannerExpression] = str :: search :: replacement :: Nil

  override private[flink] def resultType: TypeInformation[_] = STRING_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO)

  override def toString: String = s"($str).replace($search, $replacement)"

}
