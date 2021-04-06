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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.runtime.state.{StateInitializationContext, StateSnapshotContext}
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.planner.JLong

object TimeTestUtil {

  class EventTimeSourceFunction[T](
      dataWithTimestampList: Seq[Either[(Long, T), Long]]) extends SourceFunction[T] {

    override def run(ctx: SourceContext[T]): Unit = {
      dataWithTimestampList.foreach {
        case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
        case Right(w) => ctx.emitWatermark(new Watermark(w))
      }
    }

    override def cancel(): Unit = ???
  }

  class TimestampAndWatermarkWithOffset[T <: Product](
      offset: Long) extends AssignerWithPunctuatedWatermarks[T] {

    override def checkAndGetNextWatermark(lastElement: T, extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - offset)
    }

    override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
      element.productElement(0).asInstanceOf[Number].longValue()
    }
  }

  /**
   * A streaming operator to emit records and watermark depends on the input data.
   * The last emitted watermark will be stored in state to emit it again on recovery.
   * This is necessary for late arrival testing with [[FailingCollectionSource]].
   */
  class EventTimeProcessOperator[T]
    extends AbstractStreamOperator[T]
      with OneInputStreamOperator[Either[(Long, T), Long], T] {

    private var currentWatermark: Long = 0L
    private var watermarkState: ListState[JLong] = _

    override def snapshotState(context: StateSnapshotContext): Unit = {
      super.snapshotState(context)
      watermarkState.clear()
      watermarkState.add(currentWatermark)
    }

    override def initializeState(context: StateInitializationContext): Unit = {
      super.initializeState(context)
      watermarkState = context.getOperatorStateStore.getListState(
        new ListStateDescriptor("watermark-state", Types.LONG))

      val iterator = watermarkState.get().iterator()
      if (iterator.hasNext) {
        // there should be only one element in the state list, because we won't rescale in tests,
        currentWatermark = iterator.next()
      }

      if (currentWatermark > 0) {
        output.emitWatermark(new Watermark(currentWatermark))
      }
    }

    override def processElement(element: StreamRecord[Either[(Long, T), Long]]): Unit = {
      element.getValue match {
        case Left(t) =>
          output.collect(new StreamRecord[T](t._2, t._1))
        case Right(w) =>
          currentWatermark = w
          output.emitWatermark(new Watermark(w))
      }
    }

  }

}
