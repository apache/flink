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

package org.apache.flink.table.planner.expressions.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row

import org.apache.commons.lang3.StringUtils

import java.lang.{Long => JLong}
import java.util.Random

import scala.annotation.varargs
import scala.collection.mutable
import scala.io.Source

case class SimplePojo(name: String, age: Int)

@SerialVersionUID(1L)
object Func0 extends ScalarFunction {
  def eval(index: Integer): Int = {
    index
  }
}

@SerialVersionUID(1L)
object Func1 extends ScalarFunction {
  def eval(index: Integer): Integer = {
    index + 1
  }

  def eval(b: Byte): Byte = (b + 1).toByte

  def eval(s: Short): Short = (s + 1).toShort

  def eval(f: Float): Float = f + 1
}

@SerialVersionUID(1L)
object Func3 extends ScalarFunction {
  def eval(index: Integer, str: String): String = {
    s"$index and $str"
  }
}

@SerialVersionUID(1L)
object Func8 extends ScalarFunction {
  def eval(a: Int): String = {
    "a"
  }

  def eval(a: Int, b: Int): String = {
    "b"
  }

  def eval(a: String, b: String): String = {
    "c"
  }
}

@SerialVersionUID(1L)
object ShouldNotExecuteFunc extends ScalarFunction {
  def eval(s: String): Boolean = {
    throw new Exception("This func should never be executed")
  }
}

@SerialVersionUID(1L)
class RichFunc1 extends ScalarFunction {
  var added: Int = Int.MaxValue

  override def open(context: FunctionContext): Unit = {
    added = context.getJobParameter("int.value", "0").toInt
    if (context.getJobParameter("fail-for-cached-file", "false").toBoolean) {
      context.getCachedFile("FAIL")
    }
  }

  def eval(index: Int): Int = {
    index + added
  }

  override def close(): Unit = {
    added = Int.MaxValue
  }
}

@SerialVersionUID(1L)
class RichFunc2 extends ScalarFunction {
  var prefix = "ERROR_VALUE"

  override def open(context: FunctionContext): Unit = {
    prefix = context.getJobParameter("string.value", "")
  }

  def eval(value: String): String = {
    prefix + "#" + value
  }

  override def close(): Unit = {
    prefix = "ERROR_VALUE"
  }
}

@SerialVersionUID(1L)
class RichFunc3 extends ScalarFunction {
  private val words = mutable.HashSet[String]()

  override def open(context: FunctionContext): Unit = {
    val file = context.getCachedFile("words")
    for (line <- Source.fromFile(file.getCanonicalPath).getLines) {
      words.add(line.trim)
    }
  }

  def eval(value: String): Boolean = {
    words.contains(value)
  }

  override def close(): Unit = {
    words.clear()
  }
}

@SerialVersionUID(1L)
class Func13(prefix: String) extends ScalarFunction {
  def eval(a: String): String = {
    s"$prefix-$a"
  }
}

@SerialVersionUID(1L)
object Func15 extends ScalarFunction {

  @varargs
  def eval(a: String, b: Int*): String = {
    a + b.length
  }

  def eval(a: String): String = {
    a
  }
}

@SerialVersionUID(1L)
object Func18 extends ScalarFunction {
  def eval(str: String, prefix: String): Boolean = {
    str.startsWith(prefix)
  }
}

@SerialVersionUID(1L)
object Func23 extends ScalarFunction {

  @DataTypeHint("ROW<f0 STRING, f1 INT, f2 BIGINT, f3 STRING>")
  def eval(a: Integer, b: JLong, c: String): Row = {
    Row.of("star", a, b, c)
  }
}

@SerialVersionUID(1L)
object Func25 extends ScalarFunction {
  private val random = new Random()

  def eval(a: Integer): Row = {
    val col = random.nextInt()
    Row.of(Integer.valueOf(a + col), Integer.valueOf(a + col))
  }

  override def isDeterministic: Boolean = false

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.INT, Types.INT)
}

@SerialVersionUID(1L)
object Func24 extends ScalarFunction {

  @DataTypeHint("ROW<f0 STRING, f1 INT, f2 BIGINT, f3 STRING>")
  def eval(a: String, b: Integer, c: JLong, d: String): Row = {
    Row.of(a, Integer.valueOf(b + 1), c, d)
  }
}


/**
  * A scalar function that always returns TRUE if opened correctly.
  */
@SerialVersionUID(1L)
class FuncWithOpen extends ScalarFunction {

  private var permitted: Boolean = false

  override def open(context: FunctionContext): Unit = {
    permitted = true
  }

  def eval(x: Integer): Boolean = {
    permitted
  }

  override def close(): Unit = {
    permitted = false
  }
}

@SerialVersionUID(1L)
class SplitUDF(deterministic: Boolean) extends ScalarFunction {
  def eval(x: String, sep: String, index: Int): String = {
    val splits = StringUtils.splitByWholeSeparator(x, sep)
    if (splits.length > index) {
      splits(index)
    } else {
      null
    }
  }
  override def isDeterministic: Boolean = deterministic

}
