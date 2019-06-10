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

package org.apache.flink.table.runtime.utils

import java.lang.{Iterable => JIterable}
import org.apache.flink.api.java.tuple.{Tuple1, Tuple2}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext, ScalarFunction}
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{ListTypeInfo, PojoField, PojoTypeInfo, RowTypeInfo}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.dataformat.{BaseRow, BinaryString}
import org.apache.flink.types.Row

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.varargs

object UserDefinedFunctionTestUtils {

  // ------------------------------------------------------------------------------------
  // AggregateFunctions
  // ------------------------------------------------------------------------------------

  class MyPojoAggFunction extends AggregateFunction[MyPojo, CountAccumulator] {

    def accumulate(acc: CountAccumulator, value: MyPojo): Unit = {
      if (value != null) {
        acc.f0 += value.f2
      }
    }

    def retract(acc: CountAccumulator, value: MyPojo): Unit = {
      if (value != null) {
        acc.f0 -= value.f2
      }
    }

    override def getValue(acc: CountAccumulator): MyPojo = {
      new MyPojo(acc.f0.asInstanceOf[Int], acc.f0.asInstanceOf[Int])
    }

    def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
      val iter = its.iterator()
      while (iter.hasNext) {
        acc.f0 += iter.next().f0
      }
    }

    override def createAccumulator(): CountAccumulator = {
      new CountAccumulator
    }

    def resetAccumulator(acc: CountAccumulator): Unit = {
      acc.f0 = 0L
    }
  }

  /** The initial accumulator for count aggregate function */
  class CountAccumulator extends Tuple1[Long] {
    f0 = 0L //count
  }


  class VarArgsAggFunction extends AggregateFunction[Long, CountAccumulator] {

    @varargs
    def accumulate(acc: CountAccumulator, value: Long, args: String*): Unit = {
      acc.f0 += value
      args.foreach(s => acc.f0 += s.toLong)
    }

    @varargs
    def retract(acc: CountAccumulator, value: Long, args: String*): Unit = {
      acc.f0 -= value
      args.foreach(s => acc.f0 -= s.toLong)
    }

    override def getValue(acc: CountAccumulator): Long = {
      acc.f0
    }

    def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
      val iter = its.iterator()
      while (iter.hasNext) {
        acc.f0 += iter.next().f0
      }
    }

    override def createAccumulator(): CountAccumulator = {
      new CountAccumulator
    }

    def resetAccumulator(acc: CountAccumulator): Unit = {
      acc.f0 = 0L
    }
  }


  /** Counts how often the first argument was larger than the second argument. */
  class LargerThanCount extends AggregateFunction[Long, Tuple1[Long]] {

    def accumulate(acc: Tuple1[Long], a: Long, b: Long): Unit = {
      if (a > b) acc.f0 += 1
    }

    def retract(acc: Tuple1[Long], a: Long, b: Long): Unit = {
      if (a > b) acc.f0 -= 1
    }

    override def createAccumulator(): Tuple1[Long] = Tuple1.of(0L)

    override def getValue(acc: Tuple1[Long]): Long = acc.f0
  }

  class CountNullNonNull extends AggregateFunction[String, Tuple2[Long, Long]] {

    override def createAccumulator(): Tuple2[Long, Long] = Tuple2.of(0L, 0L)

    override def getValue(acc: Tuple2[Long, Long]): String = s"${acc.f0}|${acc.f1}"

    def accumulate(acc: Tuple2[Long, Long], v: String): Unit = {
      if (v == null) {
        acc.f1 += 1
      } else {
        acc.f0 += 1
      }
    }

    def retract(acc: Tuple2[Long, Long], v: String): Unit = {
      if (v == null) {
        acc.f1 -= 1
      } else {
        acc.f0 -= 1
      }
    }
  }

  class CountPairs extends AggregateFunction[Long, Tuple1[Long]] {

    def accumulate(acc: Tuple1[Long], a: String, b: String): Unit = {
      acc.f0 += 1
    }

    def retract(acc: Tuple1[Long], a: String, b: String): Unit = {
      acc.f0 -= 1
    }

    override def createAccumulator(): Tuple1[Long] = Tuple1.of(0L)

    override def getValue(acc: Tuple1[Long]): Long = acc.f0
  }

  // ------------------------------------------------------------------------------------
  // ScalarFunctions
  // ------------------------------------------------------------------------------------

  object MyHashCode extends ScalarFunction {
    def eval(s: String): Int = s.hashCode()
  }

  object OldHashCode extends ScalarFunction {
    def eval(s: String): Int = -1
  }

  object StringFunction extends ScalarFunction {
    def eval(s: String): String = s
  }

  object MyStringFunc extends ScalarFunction {
    def eval(s: String): String = s + "haha"
  }

  object BinaryStringFunction extends ScalarFunction {
    def eval(s: BinaryString): BinaryString = s
  }

  object DateFunction extends ScalarFunction {
    def eval(d: Integer): Integer = d

    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
      SqlTimeTypeInfo.DATE
  }

  // Understand type: Row wrapped as TypeInfoWrappedDataType.
  object RowFunc extends ScalarFunction {
    def eval(s: String): Row = Row.of(s)

    override def getResultType(signature: Array[Class[_]]) =
      new RowTypeInfo(Types.STRING)
  }

  object RowToStrFunc extends ScalarFunction {
    def eval(s: BaseRow): String = s.getString(0).toString
  }

  // generic.
  object ListFunc extends ScalarFunction {
    def eval(s: String): java.util.List[String] = util.Arrays.asList(s)

    override def getResultType(signature: Array[Class[_]]) =
      new ListTypeInfo(Types.STRING)
  }

  // internal but wrapped as TypeInfoWrappedDataType.
  object StringFunc extends ScalarFunction {
    def eval(s: String): String = s

    override def getResultType(signature: Array[Class[_]]): TypeInformation[String] =
      Types.STRING
  }

  object MyPojoFunc extends ScalarFunction {
    def eval(s: MyPojo): Int = s.f2

    override def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] =
      Array(MyToPojoFunc.getResultType(signature))
  }

  object MyToPojoFunc extends ScalarFunction {
    def eval(s: Int): MyPojo = new MyPojo(s, s)

    override def getResultType(signature: Array[Class[_]]): PojoTypeInfo[MyPojo] = {
      val cls = classOf[MyPojo]
      new PojoTypeInfo[MyPojo](classOf[MyPojo], util.Arrays.asList(
        new PojoField(cls.getDeclaredField("f1"), Types.INT),
        new PojoField(cls.getDeclaredField("f2"), Types.INT)))
    }
  }

  object ToCompositeObj extends ScalarFunction {
    def eval(id: Int, name: String, age: Int): CompositeObj = {
      CompositeObj(id, name, age, "0.0")
    }

    def eval(id: Int, name: String, age: Int, point: String): CompositeObj = {
      CompositeObj(id, name, age, point)
    }
  }

  object TestWrapperUdf extends ScalarFunction {
    def eval(id: Int): Int = {
      id
    }

    def eval(id: String): String = {
      id
    }
  }

  class TestAddWithOpen extends ScalarFunction {

    var isOpened: Boolean = false

    override def open(context: FunctionContext): Unit = {
      super.open(context)
      isOpened = true
      TestAddWithOpen.aliveCounter.incrementAndGet()
    }

    def eval(a: Long, b: Long): Long = {
      if (!isOpened) {
        throw new IllegalStateException("Open method is not called.")
      }
      a + b
    }
    
    def eval(a: Long, b: Int): Long = {
      eval(a, b.asInstanceOf[Long])
    }

    override def close(): Unit = {
      TestAddWithOpen.aliveCounter.decrementAndGet()
    }
  }

  object TestAddWithOpen {
    /** A thread-safe counter to record how many alive TestAddWithOpen UDFs */
    val aliveCounter = new AtomicInteger(0)
  }

  object TestMod extends ScalarFunction {
    def eval(src: Long, mod: Int): Long = {
      src % mod
    }
  }

  object TestExceptionThrown extends ScalarFunction {
    def eval(src: String): Int = {
      throw new NumberFormatException("Cannot parse this input.")
    }
  }

  // ------------------------------------------------------------------------------------
  // POJOs
  // ------------------------------------------------------------------------------------

  class MyPojo() {
    var f1: Int = 0
    var f2: Int = 0

    def this(f1: Int, f2: Int) {
      this()
      this.f1 = f1
      this.f2 = f2
    }

    override def equals(other: Any): Boolean = other match {
      case that: MyPojo =>
        (that canEqual this) &&
          f1 == that.f1 &&
          f2 == that.f2
      case _ => false
    }

    def canEqual(other: Any): Boolean = other.isInstanceOf[MyPojo]

    override def toString = s"MyPojo($f1, $f2)"
  }

  case class CompositeObj(id: Int, name: String, age: Int, point: String)

  // ------------------------------------------------------------------------------------
  // Utils
  // ------------------------------------------------------------------------------------

  def setJobParameters(env: ExecutionEnvironment, parameters: Map[String, String]): Unit = {
    val conf = new Configuration()
    parameters.foreach {
      case (k, v) => conf.setString(k, v)
    }
    env.getConfig.setGlobalJobParameters(conf)
  }

  def setJobParameters(env: StreamExecutionEnvironment, parameters: Map[String, String]): Unit = {
    val conf = new Configuration()
    parameters.foreach {
      case (k, v) => conf.setString(k, v)
    }
    env.getConfig.setGlobalJobParameters(conf)
  }

  def setJobParameters(
      env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment,
      parameters: Map[String, String]): Unit = {
    val conf = new Configuration()
    parameters.foreach {
      case (k, v) => conf.setString(k, v)
    }
    env.getConfig.setGlobalJobParameters(conf)
  }

  def writeCacheFile(fileName: String, contents: String): String = {
    val tempFile = File.createTempFile(this.getClass.getName + "-" + fileName, "tmp")
    tempFile.deleteOnExit()
    Files.write(contents, tempFile, Charsets.UTF_8)
    tempFile.getAbsolutePath
  }
}
