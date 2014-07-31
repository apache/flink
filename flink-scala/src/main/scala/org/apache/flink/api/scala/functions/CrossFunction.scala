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
import org.apache.flink.api.scala.analysis.UDF2

import org.apache.flink.api.java.record.functions.{CrossFunction => JCrossFunction}
import org.apache.flink.types.Record
import org.apache.flink.util.Collector

abstract class CrossFunctionBase[LeftIn: UDT, RightIn: UDT, Out: UDT] extends JCrossFunction with Serializable {
  val leftInputUDT = implicitly[UDT[LeftIn]]
  val rightInputUDT = implicitly[UDT[RightIn]]
  val outputUDT = implicitly[UDT[Out]]
  val udf: UDF2[LeftIn, RightIn, Out] = new UDF2(leftInputUDT, rightInputUDT, outputUDT)

  protected lazy val leftDeserializer: UDTSerializer[LeftIn] = udf.getLeftInputDeserializer
  protected lazy val leftForwardFrom: Array[Int] = udf.getLeftForwardIndexArrayFrom
  protected lazy val leftForwardTo: Array[Int] = udf.getLeftForwardIndexArrayTo
  protected lazy val leftDiscard: Array[Int] = udf.getLeftDiscardIndexArray.filter(_ < udf.getOutputLength)
  protected lazy val rightDeserializer: UDTSerializer[RightIn] = udf.getRightInputDeserializer
  protected lazy val rightForwardFrom: Array[Int] = udf.getRightForwardIndexArrayFrom
  protected lazy val rightForwardTo: Array[Int] = udf.getRightForwardIndexArrayTo
  protected lazy val serializer: UDTSerializer[Out] = udf.getOutputSerializer
  protected lazy val outputLength: Int = udf.getOutputLength

}

abstract class CrossFunction[LeftIn: UDT, RightIn: UDT, Out: UDT] extends CrossFunctionBase[LeftIn, RightIn, Out] with Function2[LeftIn, RightIn, Out] {
  override def cross(leftRecord: Record, rightRecord: Record) : Record = {
    val left = leftDeserializer.deserializeRecyclingOn(leftRecord)
    val right = rightDeserializer.deserializeRecyclingOn(rightRecord)
    val output = apply(left, right)

    leftRecord.setNumFields(outputLength)

    for (field <- leftDiscard)
      leftRecord.setNull(field)

    leftRecord.copyFrom(rightRecord, rightForwardFrom, rightForwardTo)
    leftRecord.copyFrom(leftRecord, leftForwardFrom, leftForwardTo)

    serializer.serialize(output, leftRecord)
    leftRecord
  }
}


