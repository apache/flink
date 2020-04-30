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

package org.apache.flink.table.expressions.utils

import java.lang.{Long => JLong}
import java.sql.{Date, Time, Timestamp}
import java.util.Random

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row
import org.junit.Assert

import scala.annotation.varargs
import scala.collection.mutable
import scala.io.Source

case class SimplePojo(name: String, age: Int)

object Func0 extends ScalarFunction {
  def eval(index: Int): Int = {
    index
  }
}

object Func1 extends ScalarFunction {
  def eval(index: Integer): Integer = {
    index + 1
  }

  def eval(b: Byte): Byte = (b + 1).toByte

  def eval(s: Short): Short = (s + 1).toShort

  def eval(f: Float): Float = f + 1
}

object Func2 extends ScalarFunction {
  def eval(index: Integer, str: String, pojo: SimplePojo): String = {
    s"$index and $str and $pojo"
  }
}

object Func3 extends ScalarFunction {
  def eval(index: Integer, str: String): String = {
    s"$index and $str"
  }
}

object Func4 extends ScalarFunction {
  def eval(): Integer = {
    null
  }
}

object Func5 extends ScalarFunction {
  def eval(): Int = {
    -1
  }
}

object Func6 extends ScalarFunction {
  def eval(date: Date, time: Time, timestamp: Timestamp): (Date, Time, Timestamp) = {
    (date, time, timestamp)
  }
}

object Func7 extends ScalarFunction {
  def eval(a: Integer, b: Integer): Integer = {
    a + b
  }
}

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

object Func9 extends ScalarFunction {
  def eval(a: Int, b: Int, c: Long): String = {
    s"$a and $b and $c"
  }
}

object Func10 extends ScalarFunction {
  def eval(c: Long): Long = {
    c
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.SQL_TIMESTAMP
  }
}

object Func11 extends ScalarFunction {
  def eval(a: Int, b: Long): String = {
    s"$a and $b"
  }
}

object Func12 extends ScalarFunction {
  def eval(a: Long): Long = {
    a
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.INTERVAL_MILLIS
  }
}

object ShouldNotExecuteFunc extends ScalarFunction {
  def eval(s: String): Boolean = {
    throw new Exception("This func should never be executed")
  }
}

class RichFunc0 extends ScalarFunction {
  var openCalled = false
  var closeCalled = false

  override def open(context: FunctionContext): Unit = {
    super.open(context)
    if (openCalled) {
      Assert.fail("Open called more than once.")
    } else {
      openCalled = true
    }
    if (closeCalled) {
      Assert.fail("Close called before open.")
    }
  }

  def eval(index: Int): Int = {
    if (!openCalled) {
      Assert.fail("Open was not called before eval.")
    }
    if (closeCalled) {
      Assert.fail("Close called before eval.")
    }

    index + 1
  }

  override def close(): Unit = {
    super.close()
    if (closeCalled) {
      Assert.fail("Close called more than once.")
    } else {
      closeCalled = true
    }
    if (!openCalled) {
      Assert.fail("Open was not called before close.")
    }
  }
}

class RichFunc1 extends ScalarFunction {
  var added = Int.MaxValue

  override def open(context: FunctionContext): Unit = {
    added = context.getJobParameter("int.value", "0").toInt
  }

  def eval(index: Int): Int = {
    index + added
  }

  override def close(): Unit = {
    added = Int.MaxValue
  }
}

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

class Func13(prefix: String) extends ScalarFunction {
  def eval(a: String): String = {
    s"$prefix-$a"
  }
}

object Func14 extends ScalarFunction {

  @varargs
  def eval(a: Int*): Int = {
    a.sum
  }
}

object Func15 extends ScalarFunction {

  @varargs
  def eval(a: String, b: Int*): String = {
    a + b.length
  }

  def eval(a: String): String = {
    a
  }
}

object Func16 extends ScalarFunction {

  def eval(a: Seq[String]): String = {
    a.mkString(", ")
  }
}

object Func17 extends ScalarFunction {

  // Without @varargs, we will throw an exception
  def eval(a: String*): String = {
    a.mkString(", ")
  }
}

object Func18 extends ScalarFunction {
  def eval(str: String, prefix: String): Boolean = {
    str.startsWith(prefix)
  }
}

object Func19 extends ScalarFunction {
  def eval(row: Row): Row = {
    row
  }

  override def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] =
    Array(Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT)))

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT))

}

/**
  * A scalar function that always returns TRUE if opened correctly.
  */
class Func20 extends ScalarFunction {

  private var permitted: Boolean = false

  override def open(context: FunctionContext): Unit = {
    permitted = true
  }

  def eval(x: Int): Boolean = {
    permitted
  }

  override def close(): Unit = {
    permitted = false
  }
}

object Func21 extends ScalarFunction {
  def eval(p: People): String = {
    p.name
  }

  def eval(p: Student): String = {
    "student#" + p.name
  }
}

object Func22 extends ScalarFunction {
  def eval(a: Array[People]): String = {
    a.head.name
  }

  def eval(a: Array[Student]): String = {
    "student#" + a.head.name
  }
}

object Func23 extends ScalarFunction {
  def eval(a: Integer, b: JLong, c: String): Row = {
    Row.of("star", a, b, c)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.STRING, Types.INT, Types.LONG, Types.STRING)
}

object Func24 extends ScalarFunction {
  def eval(a: String, b: Integer, c: JLong, d: String): Row = {
    Row.of(a, Integer.valueOf(b + 1), c, d)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.STRING, Types.INT, Types.LONG, Types.STRING)
}

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

class People(val name: String)

class Student(name: String) extends People(name)

class GraduatedStudent(name: String) extends Student(name)
