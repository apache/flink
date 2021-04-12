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

import org.apache.flink.table.api.config.ExecutionConfigOptions.{TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, TABLE_EXEC_MINIBATCH_ENABLED, TABLE_EXEC_MINIBATCH_SIZE}
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}

import org.junit.runners.Parameterized

import java.time.Duration
import java.util

import scala.collection.JavaConversions._

abstract class StreamingWithMiniBatchTestBase(
    miniBatch: MiniBatchMode,
    state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  override def before(): Unit = {
    super.before()
    // set mini batch
    val tableConfig = tEnv.getConfig
    miniBatch match {
      case MiniBatchOn =>
        tableConfig.getConfiguration.setBoolean(TABLE_EXEC_MINIBATCH_ENABLED, true)
        tableConfig.getConfiguration.set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
        tableConfig.getConfiguration.setLong(TABLE_EXEC_MINIBATCH_SIZE, 3L)
      case MiniBatchOff =>
        tableConfig.getConfiguration.removeConfig(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY)
    }
  }
}

object StreamingWithMiniBatchTestBase {

  case class MiniBatchMode(on: Boolean) {
    override def toString: String = {
      if (on) {
        "MiniBatch=ON"
      } else {
        "MiniBatch=OFF"
      }
    }
  }

  val MiniBatchOff = MiniBatchMode(false)
  val MiniBatchOn = MiniBatchMode(true)

  @Parameterized.Parameters(name = "{0}, StateBackend={1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(MiniBatchOff, HEAP_BACKEND),
      Array(MiniBatchOff, ROCKSDB_BACKEND),
      Array(MiniBatchOn, HEAP_BACKEND),
      Array(MiniBatchOn, ROCKSDB_BACKEND))
  }

}
