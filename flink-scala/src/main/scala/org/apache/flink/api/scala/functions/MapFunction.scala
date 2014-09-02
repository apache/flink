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

import org.apache.flink.api.scala.analysis.{UDTSerializer, UDT}
import org.apache.flink.api.scala.analysis.UDF1

import org.apache.flink.api.java.record.functions.{MapFunction => JMapFunction}
import org.apache.flink.types.Record
import org.apache.flink.util.Collector

abstract class MapFunctionBase[In: UDT, Out: UDT] extends JMapFunction with Serializable{
  val inputUDT: UDT[In] = implicitly[UDT[In]]
  val outputUDT: UDT[Out] = implicitly[UDT[Out]]
  val udf: UDF1[In, Out] = new UDF1(inputUDT, outputUDT)

  protected lazy val deserializer: UDTSerializer[In] = udf.getInputDeserializer
  protected lazy val serializer: UDTSerializer[Out] = udf.getOutputSerializer
  protected lazy val discard: Array[Int] = udf.getDiscardIndexArray
  protected lazy val outputLength: Int = udf.getOutputLength
}

abstract class MapFunction[In: UDT, Out: UDT] extends MapFunctionBase[In, Out] with Function1[In, Out] {
  override def map(record: Record, out: Collector[Record]) = {
    val input = deserializer.deserializeRecyclingOn(record)
    val output = apply(input)

    record.setNumFields(outputLength)

    for (field <- discard)
      record.setNull(field)

    serializer.serialize(output, record)
    out.collect(record)
  }
}

abstract class FlatMapFunction[In: UDT, Out: UDT] extends MapFunctionBase[In, Out] with Function1[In, Iterator[Out]] {
  override def map(record: Record, out: Collector[Record]) = {
    val input = deserializer.deserializeRecyclingOn(record)
    val output = apply(input)

    if (output.nonEmpty) {

      record.setNumFields(outputLength)

      for (field <- discard)
        record.setNull(field)

      for (item <- output) {

        serializer.serialize(item, record)
        out.collect(record)
      }
    }
  }
}

abstract class FilterFunction[In: UDT, Out: UDT] extends MapFunctionBase[In, Out] with Function1[In, Boolean]  {
  override def map(record: Record, out: Collector[Record]) = {
    val input = deserializer.deserializeRecyclingOn(record)
    if (apply(input)) {
      out.collect(record)
    }
  }
}
