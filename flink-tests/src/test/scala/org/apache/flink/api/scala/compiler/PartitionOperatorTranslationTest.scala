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
package org.apache.flink.api.scala.compiler

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.optimizer.plan.SingleInputPlanNode
import org.apache.flink.optimizer.util.CompilerTestBase
import org.apache.flink.runtime.operators.shipping.ShipStrategyType

import org.assertj.core.api.Assertions.{assertThat, fail}
import org.junit.jupiter.api.Test

class PartitionOperatorTranslationTest extends CompilerTestBase {

  @Test
  def testPartitionOperatorPreservesFields() {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val data = env.fromElements((0L, 0L))
      data
        .partitionCustom(
          new Partitioner[Long]() {
            def partition(key: Long, numPartitions: Int): Int = key.intValue()
          },
          1)
        .groupBy(1)
        .reduceGroup(x => x)
        .output(new DiscardingOutputFormat[Iterator[(Long, Long)]])

      val p = env.createProgramPlan()
      val op = compileNoStats(p)

      val sink = op.getDataSinks.iterator().next()
      val reducer = sink.getInput.getSource.asInstanceOf[SingleInputPlanNode]
      val partitioner = reducer.getInput.getSource.asInstanceOf[SingleInputPlanNode]

      assertThat(reducer.getInput.getShipStrategy).isEqualTo(ShipStrategyType.FORWARD)
      assertThat(partitioner.getInput.getShipStrategy).isEqualTo(ShipStrategyType.PARTITION_CUSTOM)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        fail(e.getMessage)
      }
    }
  }
}
