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
package org.apache.flink.table.runtime.functions

import java.lang.{StringBuilder, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.util.regex.{Matcher, Pattern}

import org.apache.flink.table.utils.EncodingUtils

import scala.annotation.varargs

/**
  * Built-in scalar runtime functions.
  *
  * NOTE: Before you add functions here, check if Calcite provides it in
  * [[org.apache.calcite.runtime.SqlFunctions]]. Furthermore, make sure to implement the function
  * efficiently. Sometimes it makes sense to create a
  * [[org.apache.flink.table.codegen.calls.CallGenerator]] instead to avoid massive object
  * creation and reuse instances.
  */
class ScalarFunctions {}

object ScalarFunctions {

  def power(a: Double, b: JBigDecimal): Double = {
    Math.pow(a, b.doubleValue())
  }

  /**
    * Returns the hyperbolic cosine of a big decimal value.
    */
  def cosh(x: JBigDecimal): Double = {
    Math.cosh(x.doubleValue())
  }

  /**
    * Returns the string that results from concatenating the arguments.
    * Returns NULL if any argument is NULL.
    */
  @varargs
  def concat(args: String*): String = {
    val sb = new StringBuilder
    var i = 0
    while (i < args.length) {
      if (args(i) == null) {
        return null
      }
      sb.append(args(i))
      i += 1
    }
    sb.toString
  }

  /**
    * Returns the string that results from concatenating the arguments and separator.
    * Returns NULL If the separator is NULL.
    *
    * Note: CONCAT_WS() does not skip empty strings. However, it does skip any NULL values after
    * the separator argument.
    *
    **/
  @varargs
  def concat_ws(separator: String, args: String*): String = {
    if (null == separator) {
      return null
    }

    val sb = new StringBuilder

    var i = 0

    var hasValueAppended = false

    while (i < args.length) {
      if (null != args(i)) {
        if (hasValueAppended) {
          sb.append(separator)
        }
        sb.append(args(i))
        hasValueAppended = true
      }
      i = i + 1
    }
    sb.toString
  }

  /**
    * Returns the natural logarithm of "x".
    */
  def log(x: Double): Double = {
    if (x <= 0.0) {
      throw new IllegalArgumentException(s"x of 'log(x)' must be > 0, but x = $x")
    } else {
      Math.log(x)
    }
  }

  /**
    * Calculates the hyperbolic tangent of a big decimal number.
    */
  def tanh(x: JBigDecimal): Double = {
    Math.tanh(x.doubleValue())
  }

  /**
    * Returns the hyperbolic sine of a big decimal value.
    */
  def sinh(x: JBigDecimal): Double = {
    Math.sinh(x.doubleValue())
  }

  /**
    * Returns the logarithm of "x" with base "base".
    */
  def log(base: Double, x: Double): Double = {
    if (x <= 0.0) {
      throw new IllegalArgumentException(s"x of 'log(base, x)' must be > 0, but x = $x")
    }
    if (base <= 1.0) {
      throw new IllegalArgumentException(s"base of 'log(base, x)' must be > 1, but base = $base")
    } else {
      Math.log(x) / Math.log(base)
    }
  }

  /**
    * Returns the logarithm of "x" with base 2.
    */
  def log2(x: Double): Double = {
    if (x <= 0.0) {
      throw new IllegalArgumentException(s"x of 'log2(x)' must be > 0, but x = $x")
    } else {
      Math.log(x) / Math.log(2)
    }
  }

  /**
    * Returns the string str left-padded with the string pad to a length of len characters.
    * If str is longer than len, the return value is shortened to len characters.
    */
  def lpad(base: String, len: Integer, pad: String): String = {
    if (len < 0) {
      return null
    } else if (len == 0) {
      return ""
    }

    val data = new Array[Char](len)
    val baseChars = base.toCharArray
    val padChars = pad.toCharArray

    // The length of the padding needed
    val pos = Math.max(len - base.length, 0)

    // Copy the padding
    var i = 0
    while (i < pos) {
      var j = 0
      while (j < pad.length && j < pos - i) {
        data(i + j) = padChars(j)
        j += 1
      }
      i += pad.length
    }

    // Copy the base
    i = 0
    while (pos + i < len && i < base.length) {
      data(pos + i) = baseChars(i)
      i += 1
    }

    new String(data)
  }

  /**
    * Returns the string str right-padded with the string pad to a length of len characters.
    * If str is longer than len, the return value is shortened to len characters.
    */
  def rpad(base: String, len: Integer, pad: String): String = {
    if (len < 0) {
      return null
    } else if (len == 0) {
      return ""
    }

    val data = new Array[Char](len)
    val baseChars = base.toCharArray
    val padChars = pad.toCharArray

    var pos = 0

    // Copy the base
    while (pos < base.length && pos < len) {
      data(pos) = baseChars(pos)
      pos += 1
    }

    // Copy the padding
    while (pos < len) {
      var i = 0
      while (i < pad.length && i < len - pos) {
        data(pos + i) = padChars(i)
        i += 1
      }
      pos += pad.length
    }

    new String(data)
  }


  /**
    * Returns a string resulting from replacing all substrings
    * that match the regular expression with replacement.
    */
  def regexp_replace(str: String, regex: String, replacement: String): String = {
    if (str == null || regex == null || replacement == null) {
      return null
    }

    str.replaceAll(regex, Matcher.quoteReplacement(replacement))
  }

  /**
    * Returns a string extracted with a specified regular expression and a regex match group index.
    */
  def regexp_extract(str: String, regex: String, extractIndex: Integer): String = {
    if (str == null || regex == null) {
      return null
    }

    val m = Pattern.compile(regex).matcher(str)
    if (m.find) {
      val mr = m.toMatchResult
      return mr.group(extractIndex)
    }

    null
  }

  /**
    * Returns a string extracted with a specified regular expression and
    * a optional regex match group index.
    */
  def regexp_extract(str: String, regex: String): String = {
    regexp_extract(str, regex, 0)
  }

  /**
    * Returns the base string decoded with base64.
    */
  def fromBase64(base64: String): String =
    EncodingUtils.decodeBase64ToString(base64)

  /**
    * Returns the base64-encoded result of the input string.
    */
  def toBase64(string: String): String =
    EncodingUtils.encodeStringToBase64(string)

  /**
    * Returns the hex string of a long argument.
    */
  def hex(string: Long): String = JLong.toHexString(string).toUpperCase()

  /**
    * Returns the hex string of a string argument.
    */
  def hex(string: String): String =
    EncodingUtils.hex(string).toUpperCase()

  /**
    * Returns an UUID string using Java utilities.
    */
  def uuid(): String = java.util.UUID.randomUUID().toString

  /**
    * Returns a string that repeats the base string n times.
    */
  def repeat(base: String, n: Int): String = EncodingUtils.repeat(base, n)

}
