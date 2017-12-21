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

import scala.annotation.varargs
import java.math.{BigDecimal => JBigDecimal}
import java.lang.StringBuilder
import java.{lang => jl}

/**
  * Built-in scalar runtime functions.
  */
class ScalarFunctions {}

object ScalarFunctions {

  def power(a: Double, b: JBigDecimal): Double = {
    Math.pow(a, b.doubleValue())
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
    }
    else {
      Math.log(x)
    }
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
    }
    else {
      Math.log(x) / Math.log(base)
    }
  }

  /**
    * Returns the Int number after the input number left shift n bits
    * @param input Int type
    * @param n
    * @return input << n
    */
  def shiftLeft(input: Int, n: Int): Int = {
    _shiftLeft(input, n).asInstanceOf[Int]
  }

  /**
    * Returns the Long number after the input number left shift n bits
    * @param input Long type
    * @param n
    * @return input << n
    */
  def shiftLeft(input: Long, n: Int): Long = {
    _shiftLeft(input, n).asInstanceOf[Long]
  }

  /**
    * Returns the Int number after the input number right shift n bits
    * @param input Int type
    * @param n
    * @return input >> n
    */
  def shiftRight(input: Int, n: Int): Int = {
    _shiftRight(input, n).asInstanceOf[Int]
  }

  /**
    * Returns the Long number after the input number right shift n bits
    * @param input Long type
    * @param n
    * @return input >> n
    */
  def shiftRight(input: Long, n: Int): Long = {
    _shiftRight(input, n).asInstanceOf[Long]
  }

  /**
    * Returns the number after the input number left shift n bits
    * Input must be type 'Long' or type 'Int'
    */
  private def _shiftLeft(input: Any, n: Int): Any = {
    input match {
      case l: jl.Long => l << n
      case i: jl.Integer => i << n
      case _ => throw new IllegalArgumentException(s"type of input in function 'shiftLeft(input, n)' must be Long or Integer")
    }
  }

  /**
    * Returns the number after the input number right shift n bits
    * Input must be type 'Long' or type 'Int'
    */
  private def _shiftRight(input: Any, n: Int): Any = {
    input match {
      case l: jl.Long => l >> n
      case i: jl.Integer => i >> n
      case _ => throw new IllegalArgumentException(s"type of input in function 'shiftRight(input, n)' must be Long or Integer")
    }
  }

}
