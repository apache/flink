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

package org.apache.flink.api.scala.operators.translation

import org.apache.flink.api.common.Plan
import org.apache.flink.api.common.operators.GenericDataSourceBase
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala._
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

class AggregateTranslationTest {
  @Test
  def translateAggregate(): Unit =  {
    try {
      val parallelism = 8

      val env = ExecutionEnvironment.createLocalEnvironment(parallelism)

      val initialData = env.fromElements((3.141592, "foobar", 77L))

      initialData.groupBy(0).aggregate(Aggregations.MIN, 1).and(Aggregations.SUM, 2)
        .output(new DiscardingOutputFormat[(Double, String, Long)])

      val p: Plan = env.createProgramPlan()
      val sink = p.getDataSinks.iterator.next

      val reducer= sink.getInput.asInstanceOf[GroupReduceOperatorBase[_, _, _]]

      assertEquals(1, reducer.getKeyColumns(0).length)
      assertEquals(0, reducer.getKeyColumns(0)(0))
      assertEquals(-1, reducer.getParallelism)
      assertTrue(reducer.isCombinable)
      assertTrue(reducer.getInput.isInstanceOf[GenericDataSourceBase[_, _]])
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Test caused an error: " + e.getMessage)

      }
    }
  }
}

