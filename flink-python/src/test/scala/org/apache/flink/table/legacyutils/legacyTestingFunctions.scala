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

package org.apache.flink.table.legacyutils

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext, ScalarFunction, TableFunction}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}

import java.lang.{Iterable => JIterable}
import org.junit.Assert

/*
 * Testing utils adopted from legacy planner until the Python code is updated.
 */

@deprecated
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

@deprecated
class MaxAccumulator[T] extends JTuple2[T, Boolean]

@deprecated
abstract class MaxAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, MaxAccumulator[T]] {

  override def createAccumulator(): MaxAccumulator[T] = {
    val acc = new MaxAccumulator[T]
    acc.f0 = getInitValue
    acc.f1 = false
    acc
  }

  def accumulate(acc: MaxAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      if (!acc.f1 || ord.compare(acc.f0, v) < 0) {
        acc.f0 = v
        acc.f1 = true
      }
    }
  }

  override def getValue(acc: MaxAccumulator[T]): T = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: MaxAccumulator[T], its: JIterable[MaxAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        accumulate(acc, a.f0)
      }
    }
  }

  def resetAccumulator(acc: MaxAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1 = false
  }

  override def getAccumulatorType: TypeInformation[MaxAccumulator[T]] = {
    new TupleTypeInfo(
      classOf[MaxAccumulator[T]],
      getValueTypeInfo,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }

  def getInitValue: T

  def getValueTypeInfo: TypeInformation[_]
}

@deprecated
class ByteMaxAggFunction extends MaxAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

@deprecated
class TableFunc1 extends TableFunction[String] {
  def eval(str: String): Unit = {
    if (str.contains("#")){
      str.split("#").foreach(collect)
    }
  }

  def eval(str: String, prefix: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach(s => collect(prefix + s))
    }
  }
}
