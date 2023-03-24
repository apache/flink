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

import org.apache.flink.table.planner.plan.`trait`.RelWindowProperties
import org.apache.flink.table.planner.plan.logical.WindowSpec
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

/** Test for [[FlinkRelMdWindowProperties]]. */
class FlinkRelMdWindowPropertiesTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetWindowPropertiesOnTableScan(): Unit = {
    // get window properties from statistics
    Array(studentLogicalScan, studentFlinkLogicalScan, studentStreamScan).foreach(
      scan => assertEquals(null, mq.getRelWindowProperties(scan)))

    Array(temporalTableLogicalScan, temporalTableFlinkLogicalScan, temporalTableStreamScan).foreach(
      scan =>
        assertEquals(
          createRelWindowProperties(0, 1, 2, tumbleWindowSpec, timeAttributeType),
          mq.getRelWindowProperties(scan)
        ))
  }

  @Test
  def testGetWindowPropertiesOnTableFunctionScan(): Unit = {
    Array(windowTableFunctionScan, lateralTableFunctionScan).zipWithIndex.foreach {
      case (scan, idx) =>
        assertEquals(
          Array(createRelWindowProperties(3, 4, 5, tumbleWindowSpec, proctimeType), null).apply(
            idx),
          mq.getRelWindowProperties(scan)
        )
    }
  }

  @Test
  def testGetWindowPropertiesOnWindowTableFunction(): Unit = {
    Array(streamTumbleWindowTVFRel, streamHopWindowTVFRel, streamCumulateWindowTVFRel).zipWithIndex
      .foreach {
        case (tvf, idx) =>
          assertEquals(
            createRelWindowProperties(
              5,
              6,
              7,
              Array(tumbleWindowSpec, hopWindowSpec, cumulateWindowSpec).apply(idx),
              timeAttributeType),
            mq.getRelWindowProperties(tvf)
          )
      }
  }

  @Test
  def testGetWindowPropertiesOnCalc(): Unit = {
    Array(
      keepWindowCalcOnTumbleWindowTVF,
      keepWindowCalcOnHopWindowTVF,
      keepWindowCalcOnCumulateWindowTVF).zipWithIndex
      .foreach {
        case (calc, idx) =>
          assertEquals(
            createRelWindowProperties(
              2,
              1,
              -1,
              Array(tumbleWindowSpec, hopWindowSpec, cumulateWindowSpec).apply(idx),
              proctimeType),
            mq.getRelWindowProperties(calc)
          )
      }
    Array(
      discardWindowCalcOnTumbleWindowTVF,
      discardWindowCalcOnHopWindowTVF,
      discardWindowCalcOnCumulateWindowTVF)
      .foreach(
        calc =>
          assertEquals(
            null,
            mq.getRelWindowProperties(calc)
          ))
  }

  @Test
  def testGetWindowPropertiesOnUnion(): Unit = {
    Array(unionOnWindowTVFWithSameWindowSpec, unionOnWindowTVFWithDifferentWindowSpec).zipWithIndex
      .foreach {
        case (union, idx) =>
          assertEquals(
            Array(createRelWindowProperties(5, 6, 7, tumbleWindowSpec, timeAttributeType), null)
              .apply(idx),
            mq.getRelWindowProperties(union)
          )
      }
  }

  @Test
  def testGetWindowPropertiesOnExchange(): Unit = {
    Array(hashOnTumbleWindowTVF, hashOnHopWindowTVF, hashOnCumulateWindowTVF).zipWithIndex.foreach {
      case (exchange, idx) =>
        assertEquals(
          createRelWindowProperties(
            5,
            6,
            7,
            Array(
              tumbleWindowSpec,
              hopWindowSpec,
              cumulateWindowSpec
            ).apply(idx),
            timeAttributeType),
          mq.getRelWindowProperties(exchange)
        )
    }
  }

  @Test
  def testGetWindowPropertiesOnLogicalAggregate(): Unit = {
    Array(logicalGroupWindowAggOnTumbleWindowTVF, logicalGroupAggOnTumbleWindowTVF).zipWithIndex
      .foreach {
        case (groupAgg, idx) =>
          assertEquals(
            Array(
              createRelWindowProperties(1, 2, -1, tumbleWindowSpec, proctimeType),
              null
            ).apply(idx),
            mq.getRelWindowProperties(groupAgg)
          )
      }
  }

  @Test
  def testGetWindowPropertiesOnPhysicalAggregate(): Unit = {
    Array(
      streamWindowAggOnWindowTVF,
      streamLocalWindowAggOnWindowTVF,
      streamGlobalWindowAggOnWindowTVF).zipWithIndex.foreach {
      case (groupAgg, idx) =>
        assertEquals(
          Array(
            createRelWindowProperties(2, 3, -1, tumbleWindowSpec, timeAttributeType),
            createRelWindowProperties(5, 6, 7, tumbleWindowSpec, timeAttributeType),
            createRelWindowProperties(2, 3, -1, tumbleWindowSpec, timeAttributeType)
          ).apply(idx),
          mq.getRelWindowProperties(groupAgg)
        )
    }
  }

  private def createRelWindowProperties(
      start: Int,
      end: Int,
      time: Int,
      spec: WindowSpec,
      timeAttributeType: LogicalType): RelWindowProperties = {
    RelWindowProperties.create(
      ImmutableBitSet.of(start),
      ImmutableBitSet.of(end),
      if (time >= 0) ImmutableBitSet.of(time) else ImmutableBitSet.of(),
      spec,
      timeAttributeType)
  }

}
