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

package org.apache.flink.table.runtime.utils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sinks.{CollectTableSink, TableSinkBase}
import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BaseRowUtil
import org.apache.flink.util.AbstractID

import _root_.java.util.{TimeZone, ArrayList => JArrayList}

import _root_.scala.collection.JavaConversions._

import org.junit.Before

class BatchTestBase {

  var env: StreamExecutionEnvironment = _
  val conf: TableConfig = new TableConfig

  // scala tableEnv
  var tEnv: ScalaBatchTableEnv = _

  @Before
  def before(): Unit = {
    // java env
    val javaEnv = new LocalStreamEnvironment()
    // scala env
    this.env = new StreamExecutionEnvironment(javaEnv)
    this.tEnv = ScalaBatchTableEnv.create(env)
  }

  def collectResults(table: Table): Seq[String] = {
    val tableSchema = table.getSchema
    val sink = new CollectBaseRowTableSink
    val configuredSink = sink.configure(tableSchema.getFieldNames, tableSchema.getFieldTypes)
                         .asInstanceOf[CollectBaseRowTableSink]
    val outType = configuredSink.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val typeSerializer = outType.createSerializer(new ExecutionConfig)
    val id = new AbstractID().toString
    configuredSink.init(typeSerializer, id)
    tEnv.writeToSink(table, configuredSink, "test")
    val res = env.execute()

    val accResult: JArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    val datas: Seq[BaseRow] = SerializedListAccumulator.deserializeList(accResult, typeSerializer)
    val tz = TimeZone.getTimeZone("UTC")
    datas.map(BaseRowUtil.baseRowToString(_, outType, tz, false))
  }

}

class CollectBaseRowTableSink
  extends CollectTableSink[BaseRow](
    types => new BaseRowTypeInfo(types.map(createInternalTypeFromTypeInfo): _*)) {

  override protected def copy: TableSinkBase[BaseRow] = {
    new CollectBaseRowTableSink
  }

}
