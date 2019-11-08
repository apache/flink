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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalDataStreamTableScan
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecBoundedStreamScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecDataStreamScan
import org.apache.flink.table.types.logical.{BigIntType, DoubleType}

import com.google.common.collect.ImmutableList
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdDistributionTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testDistributionOnTableScan(): Unit = {
    Array(studentLogicalScan, studentFlinkLogicalScan, studentBatchScan, studentStreamScan)
      .foreach { scan =>
        assertEquals(FlinkRelDistribution.ANY, mq.flinkDistribution(scan))
      }

    val distribution01 = FlinkRelDistribution.hash(Array(0, 1), requireStrict = false)
    val flinkLogicalScan: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("student"), flinkLogicalTraits.replace(distribution01))
    assertEquals(distribution01, mq.flinkDistribution(flinkLogicalScan))

    val batchScan: BatchExecBoundedStreamScan =
      createDataStreamScan(ImmutableList.of("student"), batchPhysicalTraits.replace(distribution01))
    assertEquals(distribution01, mq.flinkDistribution(batchScan))

    val streamScan: StreamExecDataStreamScan = createDataStreamScan(
      ImmutableList.of("student"), streamPhysicalTraits.replace(distribution01))
    assertEquals(distribution01, mq.flinkDistribution(streamScan))
  }

  @Test
  def testDistributionOnCalc(): Unit = {
    // hash on height
    val distribution4 = FlinkRelDistribution.hash(Array(4), requireStrict = false)
    val scan1: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("student"), flinkLogicalTraits.replace(distribution4))
    // height > 170
    relBuilder.push(scan1)
    val expr4 = relBuilder.call(LESS_THAN, relBuilder.field(4), relBuilder.literal(170.0))
    val calc =
      createLogicalCalc(scan1, logicalProject.getRowType, logicalProject.getProjects, List(expr4))
    assertEquals(FlinkRelDistribution.hash(Array(6), requireStrict = false),
      mq.flinkDistribution(calc))

    val distribution01 = FlinkRelDistribution.hash(Array(0, 1), requireStrict = false)
    val scan2: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("student"), flinkLogicalTraits.replace(distribution01))
    relBuilder.push(scan2)
    // projects: $0==1, $0, $1, true, 2.1, 2
    val projects1 = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      makeLiteral(2.1, new DoubleType(), isNullable = false, allowCast = true),
      makeLiteral(2L, new BigIntType(), isNullable = false, allowCast = true))
    val outputRowType = relBuilder.project(projects1).build().getRowType
    relBuilder.push(scan2)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    // calc => projects + filter: $0 <= 2
    val calc1 = createLogicalCalc(scan2, outputRowType, projects1, List(expr1))
    assertEquals(FlinkRelDistribution.hash(Array(1, 2), requireStrict = false),
      mq.flinkDistribution(calc1))

    // projects: $0==1, $0, 2.1, true, 2.1, 2
    val projects2 = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      makeLiteral(2.1, new DoubleType(), isNullable = false, allowCast = true),
      relBuilder.literal(true),
      makeLiteral(2.1, new DoubleType(), isNullable = false, allowCast = true),
      makeLiteral(2L, new BigIntType(), isNullable = false, allowCast = true))
    val calc2 = createLogicalCalc(scan2, outputRowType, projects2, List())
    assertEquals(FlinkRelDistribution.ANY, mq.flinkDistribution(calc2))
  }

  @Test
  def testDistributionOnSort(): Unit = {
    assertEquals(FlinkRelDistribution.SINGLETON, mq.flinkDistribution(batchSort))
    assertEquals(FlinkRelDistribution.SINGLETON, mq.flinkDistribution(streamSort))
  }
}

