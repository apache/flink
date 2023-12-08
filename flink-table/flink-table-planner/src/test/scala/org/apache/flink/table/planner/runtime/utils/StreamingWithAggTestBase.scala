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

import org.apache.flink.api.common.time.Time
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.runtime.utils.StreamingWithAggTestBase._
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters

import org.junit.jupiter.api.BeforeEach

import java.time.Duration
import java.util

import scala.collection.JavaConversions._

class StreamingWithAggTestBase(
    aggMode: AggMode,
    miniBatch: MiniBatchMode,
    backend: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, backend) {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    // in order to cover more code paths
    tEnv.getConfig.setIdleStateRetention(Duration.ofHours(1))
    if (aggMode.isLocalAggEnabled) {
      tEnv.getConfig.set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
        AggregatePhaseStrategy.TWO_PHASE.toString)
    } else {
      tEnv.getConfig.set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
        AggregatePhaseStrategy.ONE_PHASE.toString)
    }
  }
}

object StreamingWithAggTestBase {

  case class AggMode(isLocalAggEnabled: Boolean) {
    override def toString: String = if (isLocalAggEnabled) "ON" else "OFF"
  }

  val LocalGlobalOn = AggMode(isLocalAggEnabled = true)
  val LocalGlobalOff = AggMode(isLocalAggEnabled = false)

  @Parameters(name = "LocalGlobal={0}, {1}, StateBackend={2}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(LocalGlobalOff, MiniBatchOff, HEAP_BACKEND),
      Array(LocalGlobalOff, MiniBatchOn, HEAP_BACKEND),
      Array(LocalGlobalOn, MiniBatchOn, HEAP_BACKEND),
      Array(LocalGlobalOff, MiniBatchOff, ROCKSDB_BACKEND),
      Array(LocalGlobalOff, MiniBatchOn, ROCKSDB_BACKEND),
      Array(LocalGlobalOn, MiniBatchOn, ROCKSDB_BACKEND)
    )
  }
}
