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
package org.apache.flink.table.api.runtime.types

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import scala.collection.immutable.{NumericRange, Range}

/**
 * Kryo serializer for [[scala.collection.immutable.Range.Inclusive]].
 *
 * <p>Range.Inclusive does not have a no-arg constructor, so it cannot be properly instantiated by
 * Kryo's DefaultInstantiatorStrategy. This serializer handles the construction explicitly using
 * start, end, and step values.
 */
private[types] class RangeInclusiveSerializer extends Serializer[Range.Inclusive] {

  override def write(kryo: Kryo, output: Output, range: Range.Inclusive): Unit = {
    output.writeInt(range.start)
    output.writeInt(range.end)
    output.writeInt(range.step)
  }

  override def read(kryo: Kryo, input: Input, cls: Class[_ <: Range.Inclusive]): Range.Inclusive = {
    val start = input.readInt()
    val end = input.readInt()
    val step = input.readInt()
    new Range.Inclusive(start, end, step)
  }
}

/**
 * Kryo serializer for [[scala.collection.immutable.NumericRange.Inclusive]].
 *
 * <p>NumericRange types are generic and require the Integral evidence to be serialized alongside
 * the range values. The `num` field is private in NumericRange, so we use Java reflection to access
 * it.
 */
private[types] class NumericRangeInclusiveSerializer extends Serializer[NumericRange.Inclusive[_]] {

  private lazy val numField = {
    val f = classOf[NumericRange[_]].getDeclaredField("num")
    f.setAccessible(true)
    f
  }

  override def write(kryo: Kryo, output: Output, range: NumericRange.Inclusive[_]): Unit = {
    kryo.writeClassAndObject(output, range.start)
    kryo.writeClassAndObject(output, range.end)
    kryo.writeClassAndObject(output, range.step)
    kryo.writeClassAndObject(output, numField.get(range))
  }

  override def read(
      kryo: Kryo,
      input: Input,
      cls: Class[_ <: NumericRange.Inclusive[_]]): NumericRange.Inclusive[_] = {
    val start = kryo.readClassAndObject(input)
    val end = kryo.readClassAndObject(input)
    val step = kryo.readClassAndObject(input)
    val num = kryo.readClassAndObject(input).asInstanceOf[Integral[Any]]
    NumericRange.inclusive(start.asInstanceOf[Any], end.asInstanceOf[Any], step.asInstanceOf[Any])(
      num)
  }
}

/**
 * Kryo serializer for [[scala.collection.immutable.NumericRange.Exclusive]].
 *
 * <p>Same approach as [[NumericRangeInclusiveSerializer]] but for exclusive ranges.
 */
private[types] class NumericRangeExclusiveSerializer extends Serializer[NumericRange.Exclusive[_]] {

  private lazy val numField = {
    val f = classOf[NumericRange[_]].getDeclaredField("num")
    f.setAccessible(true)
    f
  }

  override def write(kryo: Kryo, output: Output, range: NumericRange.Exclusive[_]): Unit = {
    kryo.writeClassAndObject(output, range.start)
    kryo.writeClassAndObject(output, range.end)
    kryo.writeClassAndObject(output, range.step)
    kryo.writeClassAndObject(output, numField.get(range))
  }

  override def read(
      kryo: Kryo,
      input: Input,
      cls: Class[_ <: NumericRange.Exclusive[_]]): NumericRange.Exclusive[_] = {
    val start = kryo.readClassAndObject(input)
    val end = kryo.readClassAndObject(input)
    val step = kryo.readClassAndObject(input)
    val num = kryo.readClassAndObject(input).asInstanceOf[Integral[Any]]
    NumericRange(start.asInstanceOf[Any], end.asInstanceOf[Any], step.asInstanceOf[Any])(num)
  }
}
