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

package org.apache.flink.table.runtime.collector

import java.util
import java.util.Collections
import java.util.concurrent.BlockingQueue
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api.types.{DataTypes, InternalType, RowType}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.typeutils.{BaseRowSerializer, TypeUtils}

/**
  * The [[JoinedRowAsyncCollector]] is used to wrap left [[BaseRow]] and
  * right [[BaseRow]] to [[JoinedRow]]
  */
class JoinedRowAsyncCollector(
    val collectorQueue: BlockingQueue[JoinedRowAsyncCollector],
    val joinConditionCollector: TableAsyncCollector[BaseRow],
    val rightArity: Int,
    val leftOuterJoin: Boolean,
    val rightTypes: Array[InternalType]) extends ResultFuture[BaseRow] {

  var leftRow: BaseRow = _
  var realOutput: ResultFuture[BaseRow] = _

  val nullRow: GenericRow = new GenericRow(rightArity)
  val delegate = new DelegateResultFuture
  private val rightSer = DataTypes.createInternalSerializer(
    new RowType(rightTypes: _*)).asInstanceOf[BaseRowSerializer[BaseRow]]

  def reset(row: BaseRow, realOutput: ResultFuture[BaseRow]): Unit = {
    this.realOutput = realOutput
    this.leftRow = row
    joinConditionCollector.setInput(row)
    joinConditionCollector.setCollector(delegate)
    delegate.reset()
  }

  override def complete(collection: util.Collection[BaseRow]): Unit = {
    // call condition collector first
    try {
      joinConditionCollector.complete(collection)
    } catch {
      // we should cache the exception here to let the framework know
      case t: Throwable =>
        completeExceptionally(t)
        return
    }

    val resultCollection = delegate.collection
    if (resultCollection == null || resultCollection.isEmpty) {
      if (leftOuterJoin) {
        val outRow: BaseRow = new JoinedRow(leftRow, nullRow)
        outRow.setHeader(leftRow.getHeader)
        realOutput.complete(Collections.singleton(outRow))
      } else {
        realOutput.complete(Collections.emptyList[BaseRow]())
      }
    } else {
      // TODO: currently, collection should only contain one element
      val rightRow = resultCollection.iterator().next()
      // copy right row to avoid object reuse in async collector
      val outRow: BaseRow = new JoinedRow(leftRow, rightSer.copy(rightRow))
      outRow.setHeader(leftRow.getHeader)
      realOutput.complete(Collections.singleton(outRow))
    }
    // return this collector to the queue
    collectorQueue.put(this)
  }

  override def completeExceptionally(throwable: Throwable): Unit = {
    realOutput.completeExceptionally(throwable)
    // return this collector to the queue
    collectorQueue.put(this)
  }

  class DelegateResultFuture extends ResultFuture[BaseRow] {

    var collection: util.Collection[BaseRow] = _

    def reset(): Unit = {
      this.collection = null
    }

    override def complete(result: util.Collection[BaseRow]): Unit = {
      collection = result
    }

    override def completeExceptionally(error: Throwable): Unit = {
      JoinedRowAsyncCollector.this.completeExceptionally(error)
    }
  }
}
