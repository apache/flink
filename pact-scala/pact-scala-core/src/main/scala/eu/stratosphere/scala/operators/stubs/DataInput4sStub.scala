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

import java.io.DataInput

import eu.stratosphere.scala.analysis._

import eu.stratosphere.pact.common.io._
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.generic.io.BinaryInputFormat

/*case class ExternalProcessFixedLengthInputParameters[Out](
  val serializer: UDTSerializer[Out],
  val externalProcessCommand: Int => String,
  val numSplits: Option[Int],
  val outputLength: Int,
  val userFunction: (Array[Byte], Int) => Out)
  extends StubParameters*/

trait InputFormat4sStub[Out] {
  protected val udt: UDT[Out]
  lazy val udf: UDF0[Out] = new UDF0(udt)
  protected var serializer: UDTSerializer[Out] = _
  protected var outputLength: Int = _

  def configureSerialiser(config: Configuration) {
    outputLength = udf.getOutputLength
    this.serializer = udf.getOutputSerializer
  }
}

abstract class BinaryInput4sStub[Out] extends BinaryInputFormat[PactRecord] with InputFormat4sStub[Out] {
  protected val userFunction: DataInput => Out

  override def configure(config: Configuration) {
    super.configure(config)
    configureSerialiser(config)
  }

  override def deserialize(record: PactRecord, source: DataInput) = {
    val output = userFunction.apply(source)
    record.setNumFields(outputLength)
    serializer.serialize(output, record)
  }
}

abstract class DelimitedInput4sStub[Out] extends DelimitedInputFormat with InputFormat4sStub[Out] {
  protected val userFunction: (Array[Byte], Int, Int) => Out

  override def configure(config: Configuration) {
    super.configure(config)
    configureSerialiser(config)
  }

  override def readRecord(record: PactRecord, source: Array[Byte], offset: Int, numBytes: Int): Boolean = {
    val output = userFunction.apply(source, offset, numBytes)

    if (output != null) {
      record.setNumFields(outputLength)
      serializer.serialize(output, record)
    }

    return output != null
  }
}

abstract class FixedLengthInput4sStub[Out] extends FixedLengthInputFormat with InputFormat4sStub[Out] {
  protected val userFunction: (Array[Byte], Int) => Out

  override def configure(config: Configuration) {
    super.configure(config)
    configureSerialiser(config)
  }

  override def readBytes(record: PactRecord, source: Array[Byte], startPos: Int): Boolean = {
    val output = userFunction.apply(source, startPos)

    if (output != null) {
      record.setNumFields(outputLength)
      serializer.serialize(output, record)
    }
    
    return output != null
  }
}

/*class ExternalProcessFixedLengthInput4sStub[Out] extends ExternalProcessFixedLengthInputFormat[ExternalProcessInputSplit] {

  private var serializer: UDTSerializer[Out] = _
  private var externalProcessCommand: Int => String = _
  private var numSplits: Int = _
  private var outputLength: Int = _
  private var userFunction: (Array[Byte], Int) => Out = _

  override def configure(config: Configuration) {
    super.configure(config)
    val parameters = StubParameters.getValue[ExternalProcessFixedLengthInputParameters[Out]](config)

    this.serializer = parameters.serializer
    this.externalProcessCommand = parameters.externalProcessCommand
    this.numSplits = math.max(parameters.numSplits.getOrElse(1), 1)
    this.outputLength = parameters.outputLength
    this.userFunction = parameters.userFunction
  }

  override def createInputSplits(minNumSplits: Int): Array[GenericInputSplit] = {
    (0 until math.max(minNumSplits, numSplits)) map { split => new ExternalProcessInputSplit(split, this.externalProcessCommand(split)) } toArray
  }

  override def readBytes(record: PactRecord, source: Array[Byte], startPos: Int): Boolean = {

    val output = userFunction.apply(source, startPos)

    if (output != null) {
      record.setNumFields(outputLength)
      serializer.serialize(output, record)
    }

    return output != null
  }
}*/
