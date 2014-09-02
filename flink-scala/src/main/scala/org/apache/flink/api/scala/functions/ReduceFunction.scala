/**
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


package org.apache.flink.api.scala.functions

import scala.Iterator

import java.util.{Iterator => JIterator}

import org.apache.flink.api.scala.analysis.{UDTSerializer, FieldSelector, UDT}
import org.apache.flink.api.scala.analysis.UDF1

import org.apache.flink.api.java.record.functions.{ReduceFunction => JReduceFunction}
import org.apache.flink.types.Record
import org.apache.flink.util.Collector



abstract class ReduceFunctionBase[In: UDT, Out: UDT] extends JReduceFunction with Serializable {
  val inputUDT: UDT[In] = implicitly[UDT[In]]
  val outputUDT: UDT[Out] = implicitly[UDT[Out]]
  val udf: UDF1[In, Out] = new UDF1(inputUDT, outputUDT)

  protected val reduceRecord = new Record()

  protected lazy val reduceIterator: DeserializingIterator[In] = new DeserializingIterator(udf.getInputDeserializer)
  protected lazy val reduceSerializer: UDTSerializer[Out] = udf.getOutputSerializer
  protected lazy val reduceForwardFrom: Array[Int] = udf.getForwardIndexArrayFrom
  protected lazy val reduceForwardTo: Array[Int] = udf.getForwardIndexArrayTo
}

abstract class ReduceFunction[In: UDT] extends ReduceFunctionBase[In, In] with Function2[In, In, In] {

  override def combine(records: JIterator[Record], out: Collector[Record]) = {
    reduce(records, out)
  }

  override def reduce(records: JIterator[Record], out: Collector[Record]) = {
    val firstRecord = reduceIterator.initialize(records)
    reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

    val output = reduceIterator.reduce(apply)

    reduceSerializer.serialize(output, reduceRecord)
    out.collect(reduceRecord)
  }
}

abstract class GroupReduceFunction[In: UDT, Out: UDT] extends ReduceFunctionBase[In, Out] with Function1[Iterator[In], Out] {
  override def reduce(records: JIterator[Record], out: Collector[Record]) = {
    val firstRecord = reduceIterator.initialize(records)
    reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

    val output = apply(reduceIterator)

    reduceSerializer.serialize(output, reduceRecord)
    out.collect(reduceRecord)
  }
}

abstract class CombinableGroupReduceFunction[In: UDT, Out: UDT] extends ReduceFunctionBase[In, Out] with Function1[Iterator[In], Out] {
  override def combine(records: JIterator[Record], out: Collector[Record]) = {
    val firstRecord = reduceIterator.initialize(records)
    reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

    val output = combine(reduceIterator)

    reduceSerializer.serialize(output, reduceRecord)
    out.collect(reduceRecord)
  }

  override def reduce(records: JIterator[Record], out: Collector[Record]) = {
    val firstRecord = reduceIterator.initialize(records)
    reduceRecord.copyFrom(firstRecord, reduceForwardFrom, reduceForwardTo)

    val output = reduce(reduceIterator)

    reduceSerializer.serialize(output, reduceRecord)
    out.collect(reduceRecord)
  }

  def reduce(records: Iterator[In]): Out
  def combine(records: Iterator[In]): Out

  def apply(record: Iterator[In]): Out = throw new RuntimeException("This should never be called.")
}