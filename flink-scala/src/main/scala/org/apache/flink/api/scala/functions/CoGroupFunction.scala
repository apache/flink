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

import java.util.{Iterator => JIterator}

import org.apache.flink.api.scala.analysis.{UDTSerializer, UDT}
import org.apache.flink.api.scala.analysis.UDF2

import org.apache.flink.api.java.record.functions.{CoGroupFunction => JCoGroupFunction}
import org.apache.flink.types.Record
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration


abstract class CoGroupFunctionBase[LeftIn: UDT, RightIn: UDT, Out: UDT] extends JCoGroupFunction with Serializable {
  val leftInputUDT = implicitly[UDT[LeftIn]]
  val rightInputUDT = implicitly[UDT[RightIn]]
  val outputUDT = implicitly[UDT[Out]]
  val udf: UDF2[LeftIn, RightIn, Out] = new UDF2(leftInputUDT, rightInputUDT, outputUDT)

  protected val outputRecord = new Record()

  protected lazy val leftIterator: DeserializingIterator[LeftIn] = new DeserializingIterator(udf.getLeftInputDeserializer)
  protected lazy val leftForwardFrom: Array[Int] = udf.getLeftForwardIndexArrayFrom
  protected lazy val leftForwardTo: Array[Int] = udf.getLeftForwardIndexArrayTo
  protected lazy val rightIterator: DeserializingIterator[RightIn] = new DeserializingIterator(udf.getRightInputDeserializer)
  protected lazy val rightForwardFrom: Array[Int] = udf.getRightForwardIndexArrayFrom
  protected lazy val rightForwardTo: Array[Int] = udf.getRightForwardIndexArrayTo
  protected lazy val serializer: UDTSerializer[Out] = udf.getOutputSerializer

  override def open(config: Configuration) = {
    super.open(config)

    this.outputRecord.setNumFields(udf.getOutputLength)
  }
}

abstract class CoGroupFunction[LeftIn: UDT, RightIn: UDT, Out: UDT] extends CoGroupFunctionBase[LeftIn, RightIn, Out] with Function2[Iterator[LeftIn], Iterator[RightIn], Out] {
  override def coGroup(leftRecords: JIterator[Record], rightRecords: JIterator[Record], out: Collector[Record]) = {
    val firstLeftRecord = leftIterator.initialize(leftRecords)
    val firstRightRecord = rightIterator.initialize(rightRecords)

    if (firstRightRecord != null) {
      outputRecord.copyFrom(firstRightRecord, rightForwardFrom, rightForwardTo)
    }
    if (firstLeftRecord != null) {
      outputRecord.copyFrom(firstLeftRecord, leftForwardFrom, leftForwardTo)
    }

    val output = apply(leftIterator, rightIterator)

    serializer.serialize(output, outputRecord)
    out.collect(outputRecord)
  }
}

abstract class FlatCoGroupFunction[LeftIn: UDT, RightIn: UDT, Out: UDT] extends CoGroupFunctionBase[LeftIn, RightIn, Out] with Function2[Iterator[LeftIn], Iterator[RightIn], Iterator[Out]] {
  override def coGroup(leftRecords: JIterator[Record], rightRecords: JIterator[Record], out: Collector[Record]) = {
    val firstLeftRecord = leftIterator.initialize(leftRecords)
    outputRecord.copyFrom(firstLeftRecord, leftForwardFrom, leftForwardTo)

    val firstRightRecord = rightIterator.initialize(rightRecords)
    outputRecord.copyFrom(firstRightRecord, rightForwardFrom, rightForwardTo)

    val output = apply(leftIterator, rightIterator)

    if (output.nonEmpty) {

      for (item <- output) {
        serializer.serialize(item, outputRecord)
        out.collect(outputRecord)
      }
    }
  }
}