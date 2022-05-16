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
package org.apache.flink.streaming.scala.examples.windowing.util

import org.apache.flink.api.java.tuple.{Tuple4 => JTuple4}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.examples.windowing.util.{CarSource => JCarSource}
import org.apache.flink.streaming.scala.examples.windowing.TopSpeedWindowing.CarEvent

import java.lang.{Double => JDouble, Integer => JInt, Long => JLong}

/** A simple in-memory source. */
object CarSource {
  def apply(cars: Int): CarSource =
    new CarSource(JCarSource.create(cars))
}

class CarSource private (inner: JCarSource) extends SourceFunction[CarEvent] {

  override def run(ctx: SourceFunction.SourceContext[CarEvent]): Unit = {
    inner.run(new WrappingCollector(ctx))
  }

  override def cancel(): Unit = inner.cancel()
}

private class WrappingCollector(ctx: SourceFunction.SourceContext[CarEvent])
  extends SourceFunction.SourceContext[JTuple4[JInt, JInt, JDouble, JLong]] {

  override def collect(element: JTuple4[JInt, JInt, JDouble, JLong]): Unit =
    ctx.collect(CarEvent(element.f0, element.f1, element.f2, element.f3))

  override def collectWithTimestamp(
      element: JTuple4[JInt, JInt, JDouble, JLong],
      timestamp: Long): Unit =
    ctx.collectWithTimestamp(CarEvent(element.f0, element.f1, element.f2, element.f3), timestamp)

  override def emitWatermark(mark: Watermark): Unit = ctx.emitWatermark(mark)

  override def markAsTemporarilyIdle(): Unit = ctx.markAsTemporarilyIdle()

  override def getCheckpointLock: AnyRef = ctx.getCheckpointLock

  override def close(): Unit = ctx.close()
}
