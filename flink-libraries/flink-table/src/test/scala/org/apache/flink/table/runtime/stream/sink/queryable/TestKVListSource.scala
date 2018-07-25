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

package org.apache.flink.table.runtime.stream.sink.queryable

import org.apache.flink.streaming.api.functions.source.SourceFunction

class TestKVListSource[K, V](private val input: Seq[(K, V)]) extends SourceFunction[(K, V)] {
  @volatile private var running = true

  override def run(ctx: SourceFunction.SourceContext[(K, V)]): Unit = {
    ctx.getCheckpointLock.synchronized {
      input.foreach(ctx.collect)
    }

    while(running) {
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
