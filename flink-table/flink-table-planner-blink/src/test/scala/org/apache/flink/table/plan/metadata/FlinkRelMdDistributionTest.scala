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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.`type`.InternalTypes
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalDataStreamTableScan

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.junit.Assert._
import org.junit.Test

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._

class FlinkRelMdDistributionTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testDistributionOnCalc(): Unit = {
    val traits = logicalTraits.replace(createDistribution(Array(0, 1)))
    val scan: FlinkLogicalDataStreamTableScan =
      createDataStreamScan(ImmutableList.of("student"), traits)

    relBuilder.push(scan)
    // projects: $0==1, $0, $1, true, 2.1, 2
    val projects1 = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      relBuilder.field(1),
      relBuilder.literal(true),
      makeLiteral(2.1, InternalTypes.DOUBLE, isNullable = false, allowCast = true),
      makeLiteral(2L, InternalTypes.LONG, isNullable = false, allowCast = true))
    val outputRowType = relBuilder.project(projects1).build().getRowType
    relBuilder.push(scan)
    val expr1 = relBuilder.call(LESS_THAN_OR_EQUAL, relBuilder.field(0), relBuilder.literal(2))
    // calc => projects + filter: $0 <= 2
    val calc1 = createLogicalCalc(scan, outputRowType, projects1, List(expr1))
    assertEquals(createDistribution(Array(1, 2)), mq.flinkDistribution(calc1))

    // projects: $0==1, $0, 2.1, true, 2.1, 2
    val projects2 = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      makeLiteral(2.1, InternalTypes.DOUBLE, isNullable = false, allowCast = true),
      relBuilder.literal(true),
      makeLiteral(2.1, InternalTypes.DOUBLE, isNullable = false, allowCast = true),
      makeLiteral(2L, InternalTypes.LONG, isNullable = false, allowCast = true))
    val calc2 = createLogicalCalc(scan, outputRowType, projects2, List())
    assertEquals(FlinkRelDistribution.ANY, mq.flinkDistribution(calc2))
  }

  private def createDistribution(int: Array[Int]): FlinkRelDistribution = {
    val fields = new JArrayList[Integer]()
    for (field <- int) fields.add(field)
    FlinkRelDistribution.hash(fields, requireStrict = false)
  }
}

