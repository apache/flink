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


package org.apache.flink.api.scala.operators

import scala.language.reflectiveCalls

import org.apache.flink.api.scala.ScalaOperator
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.OneInputScalaOperator
import org.apache.flink.api.scala.analysis.UDT
import org.apache.flink.api.scala.analysis.UDF1
import org.apache.flink.api.scala.analysis.UDTSerializer

import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Record
import org.apache.flink.api.common.operators.Operator
import org.apache.flink.api.java.record.functions.MapFunction
import org.apache.flink.api.java.record.operators.MapOperator

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