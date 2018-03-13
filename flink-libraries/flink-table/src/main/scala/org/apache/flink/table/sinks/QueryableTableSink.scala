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

package org.apache.flink.table.sinks

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate.ProcessFunctionWithCleanupState
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class QueryableTableSink(private val namePrefix: String,
                         private val queryConfig: StreamQueryConfig)
  extends UpsertStreamTableSink[Row]
  with TableSinkBase[JTuple2[JBool, Row]] {
  private var keys: Array[String] = _

  /**
    * Configures the unique key fields of the [[Table]] to write.
    * The method is called after [[TableSink.configure()]].
    *
    * The keys array might be empty, if the table consists of a single (updated) record.
    * If the table does not have a key and is append-only, the keys attribute is null.
    *
    * @param keys the field names of the table's keys, an empty array if the table has a single
    *             row, and null if the table is append-only and has no key.
    */
  override def setKeyFields(keys: Array[String]): Unit = {
    if (keys == null) {
      throw new IllegalArgumentException("keys can't be null!")
    }
    this.keys = keys
  }

  /**
    * Specifies whether the [[Table]] to write is append-only or not.
    *
    * @param isAppendOnly true if the table is append-only, false otherwise.
    */
  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {
    if (isAppendOnly) {
      throw new IllegalArgumentException("A QueryableTableSink can not be used with append-only tables " +
        "as the table would grow infinitely")
    }
  }

  /** Returns the requested record type */
  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(getFieldTypes, getFieldNames)

  /** Emits the DataStream. */
  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): Unit = {
    val keyIndices = keys.map(getFieldNames.indexOf(_))
    val keyTypes = keyIndices.map(getFieldTypes(_))

    val keySelectorType = new RowTypeInfo(keyTypes, keys)

    val processFunction = new QueryableStateProcessFunction(
      namePrefix,
      queryConfig,
      keys,
      getFieldNames,
      getFieldTypes)

    dataStream.keyBy(new RowKeySelector(keyIndices, keySelectorType))
      .process(processFunction)
  }

  /** Return a deep copy of the [[TableSink]]. */
  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    new QueryableTableSink(this.namePrefix, this.queryConfig)
  }
}

class RowKeySelector(private val keyIndices: Array[Int],
                     @transient private val returnType: TypeInformation[Row])
  extends KeySelector[JTuple2[JBool, Row], Row]
    with ResultTypeQueryable[Row] {

  override def getKey(value: JTuple2[JBool, Row]): Row = {
    val keys = keyIndices

    val srcRow = value.f1

    val destRow = new Row(keys.length)
    var i = 0
    while (i < keys.length) {
      destRow.setField(i, srcRow.getField(keys(i)))
      i += 1
    }

    destRow
  }

  override def getProducedType: TypeInformation[Row] = returnType
}

class QueryableStateProcessFunction(private val namePrefix: String,
                                    private val queryConfig: StreamQueryConfig,
                                    private val keyNames: Array[String],
                                    private val fieldNames: Array[String],
                                    private val fieldTypes: Array[TypeInformation[_]])
  extends ProcessFunctionWithCleanupState[JTuple2[JBool, Row], Void](queryConfig) {

  @transient private var states = Array[ValueState[AnyRef]]()
  @transient private var nonKeyIndices = Array[Int]()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    nonKeyIndices = fieldNames.indices
      .filter(idx => !keyNames.contains(fieldNames(idx)))
      .toArray

    val statesBuilder = Array.newBuilder[ValueState[AnyRef]]

    for (i <- nonKeyIndices) {
      val stateDesc = new ValueStateDescriptor(fieldNames(i), fieldTypes(i))
      stateDesc.initializeSerializerUnlessSet(getRuntimeContext.getExecutionConfig)
      stateDesc.setQueryable(fieldNames(i))
      statesBuilder += getRuntimeContext.getState(stateDesc).asInstanceOf[ValueState[AnyRef]]
    }

    states = statesBuilder.result()

    initCleanupTimeState("QueryableStateCleanupTime")
  }

  override def processElement(value: JTuple2[JBool, Row],
                              ctx: ProcessFunction[JTuple2[JBool, Row], Void]#Context,
                              out: Collector[Void]): Unit = {
    if (value.f0) {
      for (i <- nonKeyIndices.indices) {
        states(i).update(value.f1.getField(nonKeyIndices(i)))
      }

      val currentTime = ctx.timerService().currentProcessingTime()
      registerProcessingCleanupTimer(ctx, currentTime)
    } else {
      cleanupState(states:_*)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: ProcessFunction[JTuple2[JBool, Row], Void]#OnTimerContext,
                       out: Collector[Void]): Unit = {
    if (needToCleanupState(timestamp)) {
      cleanupState(states:_*)
    }
  }
}
