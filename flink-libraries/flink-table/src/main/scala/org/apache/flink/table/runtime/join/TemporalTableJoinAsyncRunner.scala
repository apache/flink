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
package org.apache.flink.table.runtime.join

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture, RichAsyncFunction}
import org.apache.flink.table.api.types.{DataTypes, InternalType, TypeConverters}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.collector.{JoinedRowAsyncCollector, TableAsyncCollector}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging

class TemporalTableJoinAsyncRunner(
    fetcherName: String,
    var fetcherCode: String,
    collectorName: String,
    var collectorCode: String,
    capacity: Int,
    leftOuterJoin: Boolean,
    inputFieldTypes: Array[InternalType],
    @transient returnType: BaseRowTypeInfo)
  extends RichAsyncFunction[BaseRow, BaseRow]
  with ResultTypeQueryable[BaseRow]
  with Compiler[Any]
  with Logging {

  var fetcher: AsyncFunction[BaseRow, BaseRow] = _
  var collectorQueue: BlockingQueue[JoinedRowAsyncCollector] = _
  var collectorClass: Class[TableAsyncCollector[BaseRow]] = _
  val rightArity: Int = returnType.getArity - inputFieldTypes.length
  private val rightTypes =
    returnType.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo)
        .drop(inputFieldTypes.length)

  override def open(parameters: Configuration): Unit = {
    collectorClass = compile(
      getRuntimeContext.getUserCodeClassLoader,
      collectorName,
      collectorCode).asInstanceOf[Class[TableAsyncCollector[BaseRow]]]
    collectorCode = null

    val fetcherClass = compile(
      getRuntimeContext.getUserCodeClassLoader,
      fetcherName,
      fetcherCode)
    fetcherCode = null
    fetcher = fetcherClass.newInstance().asInstanceOf[AsyncFunction[BaseRow, BaseRow]]

    // add an additional collector in it to avoid blocking on the queue when taking a collector
    collectorQueue = new ArrayBlockingQueue[JoinedRowAsyncCollector](capacity + 1)
    for (_ <- 0 until capacity + 1) {
      val c = new JoinedRowAsyncCollector(
        collectorQueue,
        getTemporalTableCollector,
        rightArity,
        leftOuterJoin,
        rightTypes)
      // throws exception immediately if the queue is full which should never happen
      collectorQueue.add(c)
    }

    FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext)
    FunctionUtils.openFunction(fetcher, parameters)
  }

  override def asyncInvoke(in: BaseRow, asyncCollector: ResultFuture[BaseRow]): Unit = {
    val collector = collectorQueue.take()
    // the input row is copied when object reuse in AsyncWaitOperator
    collector.reset(in, asyncCollector)

    // fetcher has copied the input field when object reuse is enabled
    fetcher.asyncInvoke(in, collector)
  }

  protected def getTemporalTableCollector: TableAsyncCollector[BaseRow] = {
    collectorClass.newInstance()
  }

  override def getProducedType: BaseRowTypeInfo =
    returnType.asInstanceOf[BaseRowTypeInfo]

  override def close(): Unit = {
    FunctionUtils.closeFunction(fetcher)
  }
}
