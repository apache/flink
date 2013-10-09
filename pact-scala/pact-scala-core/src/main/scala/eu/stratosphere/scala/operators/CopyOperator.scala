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

package eu.stratosphere.scala.operators

import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.scala.contracts.Annotations
import eu.stratosphere.scala.OneInputScalaContract
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.analysis.UDTSerializer
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.scala.DataStream

object CopyOperator {
  def apply(source: Contract with ScalaContract[_]): DataStream[_] = {
    val generatedStub = new MapStub with Serializable {
      val udf: UDF1[_, _] = new UDF1(source.getUDF.outputUDT, source.getUDF.outputUDT)

      private var from: Array[Int] = _
      private var to: Array[Int] = _
      private var discard: Array[Int] = _
      private var outputLength: Int = _

      override def open(config: Configuration) = {
        super.open(config)
        this.from = udf.inputFields.toSerializerIndexArray
        this.to = udf.outputFields.toSerializerIndexArray
        this.discard = udf.getDiscardIndexArray.filter(_ < udf.getOutputLength)
        this.outputLength = udf.getOutputLength
      }

      override def map(record: PactRecord, out: Collector[PactRecord]) = {

        record.setNumFields(outputLength)

        record.copyFrom(record, from, to)

        for (field <- discard)
          record.setNull(field)

        out.collect(record)
      }
    }

    val builder = MapContract.builder(generatedStub).input(source)

    val ret = new MapContract(builder) with OneInputScalaContract[Nothing, Nothing] {
      override def getUDF = generatedStub.udf.asInstanceOf[UDF1[Nothing, Nothing]]
      override def annotations = Seq(Annotations.getConstantFields(generatedStub.udf.getForwardIndexArray))
      persistHints = { () =>
        this.setName("Copy " + source.getName())
        if (source.getCompilerHints().getAvgBytesPerRecord() >= 0)
          this.getCompilerHints().setAvgBytesPerRecord(source.getCompilerHints().getAvgBytesPerRecord())
      }
    }
    new DataStream[Nothing](ret)
  }
}