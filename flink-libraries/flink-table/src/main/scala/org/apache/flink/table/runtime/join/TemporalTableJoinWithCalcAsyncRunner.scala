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

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.collector.TableAsyncCollector
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector

/**
  * The async join runner with an additional calculate function on the dimension table
  */
class TemporalTableJoinWithCalcAsyncRunner(
    fetcherName: String,
    fetcherCode: String,
    calcFunctionName: String,
    var calcFunctionCode: String,
    collectorName: String,
    collectorCode: String,
    capacity: Int,
    leftOuterJoin: Boolean,
    inputFieldTypes: Array[InternalType],
    @transient returnType: BaseRowTypeInfo)
  extends TemporalTableJoinAsyncRunner(
    fetcherName,
    fetcherCode,
    collectorName,
    collectorCode,
    capacity,
    leftOuterJoin,
    inputFieldTypes,
    returnType) {

  // keep Row to keep compatible
  var calcClass: Class[FlatMapFunction[BaseRow, BaseRow]] = _

  override def open(parameters: Configuration): Unit = {
    // compile calc class first
    LOG.debug(s"Compiling CalcFunction: $calcFunctionName \n\n Code:\n$calcFunctionCode")
    calcClass = compile(
      getRuntimeContext.getUserCodeClassLoader,
      calcFunctionName,
      calcFunctionCode).asInstanceOf[Class[FlatMapFunction[BaseRow, BaseRow]]]
    calcFunctionCode = null

    super.open(parameters)
  }

  override protected def getTemporalTableCollector: TableAsyncCollector[BaseRow] = {
    val joinConditionCollector = super.getTemporalTableCollector
    new TemporalTableCalcCollector(calcClass.newInstance(), joinConditionCollector)
  }

  class TemporalTableCalcCollector(
      calcFlatMap: FlatMapFunction[BaseRow, BaseRow],
      joinConditionCollector: TableAsyncCollector[BaseRow])
    extends TableAsyncCollector[BaseRow] {

    val collectionCollector = new CalcCollectionCollector

    override def setInput(input: Any): Unit = {
      joinConditionCollector.setInput(input)
      collectionCollector.collection = null
    }

    override def setCollector(collector: ResultFuture[_]): Unit = {
      joinConditionCollector.setCollector(collector)
    }

    override def complete(collection: util.Collection[BaseRow]): Unit = {
      if (collection == null || collection.size() == 0) {
        joinConditionCollector.complete(collection)
      } else {
        // TODO: currently, collection should only contain one element
        val input = collection.iterator().next()
        calcFlatMap.flatMap(input, collectionCollector)
        joinConditionCollector.complete(collectionCollector.collection)
      }
    }

    override def completeExceptionally(throwable: Throwable): Unit =
      joinConditionCollector.completeExceptionally(throwable)
  }

  class CalcCollectionCollector extends Collector[BaseRow] {

    var collection: util.Collection[BaseRow] = _

    override def collect(t: BaseRow): Unit = {
      collection = Collections.singleton(t)
    }

    override def close(): Unit = {
    }
  }
}
