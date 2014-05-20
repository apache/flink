/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.scala.operators

import scala.language.reflectiveCalls

import eu.stratosphere.api.scala.ScalaOperator
import eu.stratosphere.api.java.record.operators.MapOperator
import eu.stratosphere.api.scala.analysis.UDT
import eu.stratosphere.types.Record
import eu.stratosphere.api.java.record.functions.MapFunction
import eu.stratosphere.util.Collector
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.scala.OneInputScalaOperator
import eu.stratosphere.api.scala.analysis.UDF1
import eu.stratosphere.api.scala.analysis.UDTSerializer
import eu.stratosphere.configuration.Configuration
import eu.stratosphere.api.scala.DataSet

object CopyOperator {
  def apply(source: Operator[Record] with ScalaOperator[_, _]): DataSet[_] = {
    val generatedStub = new MapFunction with Serializable {
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

      override def map(record: Record, out: Collector[Record]) = {

        record.setNumFields(outputLength)

        record.copyFrom(record, from, to)

        for (field <- discard)
          record.setNull(field)

        out.collect(record)
      }
    }

    val builder = MapOperator.builder(generatedStub).input(source)

    val ret = new MapOperator(builder) with OneInputScalaOperator[Nothing, Nothing] {
      override def getUDF = generatedStub.udf.asInstanceOf[UDF1[Nothing, Nothing]]
      override def annotations = Seq(Annotations.getConstantFields(
        generatedStub.udf.getForwardIndexArrayFrom.zip(generatedStub.udf.getForwardIndexArrayTo)
          .filter( z => z._1 == z._2).map { _._1}))
      persistHints = { () =>
        this.setName("Copy " + source.getName())
        if (source.getCompilerHints().getAvgOutputRecordSize() >= 0)
          this.getCompilerHints().setAvgOutputRecordSize(source.getCompilerHints().getAvgOutputRecordSize())
      }
    }
    new DataSet[Nothing](ret)
  }
}