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
package org.apache.flink.api.scala.table

import org.apache.flink.api.common.AbstractExecutionEnvironment
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.Table
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Environment for working with the Table API.
 *
 * Conversion from [[DataSet]] or [[DataStream]] to a [[Table]] happen implicitly in Scala.
 */
class TableEnvironment(environment: AbstractExecutionEnvironment) {
  require(environment != null, "The environment must not be null.")

  private def translatorFromEnv = {
    environment match {
      case batchEnv: ExecutionEnvironment =>
        new ScalaBatchTranslator(batchEnv.getJavaEnv)
      case streamEnv: StreamExecutionEnvironment =>
        new ScalaStreamingTranslator(streamEnv.getJavaEnv)
      case _ =>
        throw new IllegalArgumentException("ExecutionEnvironment is invalid for the " +
          "Scala TableEnvironment.")
    }
  }

  /**
   * Converts the [[Table]] to a [[DataSet]].
   */
  def toDataSet[T: TypeInformation](table: Table): DataSet[T] = {
    translatorFromEnv match {
      case batchTranslator: ScalaBatchTranslator =>
        batchTranslator.translate[T](table.operation)
      case _ =>
        throw new IllegalArgumentException("ExecutionEnvironment does not support Scala DataSets.")
    }
  }

  /**
   * Converts the [[Table]] to a [[DataStream]].
   */
  def toDataStream[T: TypeInformation](table: Table): DataStream[T] = {
    translatorFromEnv match {
      case streamTranslator: ScalaStreamingTranslator =>
        streamTranslator.translate[T](table.operation)
      case _ =>
        throw new IllegalArgumentException("ExecutionEnvironment does not support Scala " +
          "DataStreams.")
    }
  }
}

