/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala.operators.stubs

import java.io.DataOutput
import java.io.OutputStream

import eu.stratosphere.scala.analysis._

import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration

import eu.stratosphere.pact.generic.io.{BinaryOutputFormat => JavaBinaryOutputFormat}
import eu.stratosphere.pact.common.io.{DelimitedOutputFormat => JavaDelimitedOutputFormat}
import eu.stratosphere.pact.common.io.FileOutputFormat

trait ScalaOutputFormat[In] {
  protected val udt: UDT[In]
  lazy val udf: UDF1[In, Nothing] = new UDF1[In, Nothing](udt, UDT.NothingUDT)
  protected var deserializer: UDTSerializer[In] = _

  def configureSerialiser(config: Configuration) {
    this.deserializer = udf.getInputDeserializer
  }
}

abstract class RawOutputFormat[In] extends FileOutputFormat with ScalaOutputFormat[In] {
  protected val userFunction: (In, OutputStream) => Unit
  
  override def configure(config: Configuration) {
    super.configure(config)
    configureSerialiser(config)
  }

  override def writeRecord(record: PactRecord) = {
    val input = deserializer.deserializeRecyclingOn(record)
    userFunction.apply(input, this.stream)
  }
}

abstract class BinaryOutputFormat[In] extends JavaBinaryOutputFormat[PactRecord] with ScalaOutputFormat[In] {
  protected val userFunction: (In, DataOutput) => Unit
  
  override def configure(config: Configuration) {
    super.configure(config)
    configureSerialiser(config)
  }

  override def serialize(record: PactRecord, target: DataOutput) = {
    val input = deserializer.deserializeRecyclingOn(record)
    userFunction.apply(input, target)
  }
}

abstract class DelimitedOutputFormat[In] extends JavaDelimitedOutputFormat with ScalaOutputFormat[In] {
  protected val userFunction: (In, Array[Byte]) => Int

  override def configure(config: Configuration) {
    super.configure(config)
    configureSerialiser(config)
  }

  override def serializeRecord(record: PactRecord, target: Array[Byte]): Int = {
    val input = deserializer.deserializeRecyclingOn(record)
    userFunction.apply(input, target)
  }
}