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

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

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
}
