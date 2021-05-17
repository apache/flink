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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.planner.JBigDecimal

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.util.TimestampString
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.junit.Test

import java.util

class ExpandTest extends RelNodeTestBase {

  private val fieldNames = Array("string", "double", "long")
  private val fieldTypes = Array(
    DataTypes.STRING().getLogicalType,
    DataTypes.DOUBLE().getLogicalType,
    DataTypes.BIGINT().getLogicalType)
  private lazy val logicalTableScan = buildLogicalTableScan(fieldNames, fieldTypes)

  @Test(expected = classOf[AssertionError])
  def testUnsupportedExpandProjectsWithDifferentLength(): Unit = {
    val inputFields = logicalTableScan.getRowType.getFieldList
    val expandProjects: util.List[util.List[RexNode]] = new util.ArrayList()
    val rexBuilder = logicalTableScan.getCluster.getRexBuilder

    val project: util.List[RexNode] = new util.ArrayList[RexNode]()
    project.add(rexBuilder.makeInputRef(inputFields.get(0).getType, 0))
    project.add(rexBuilder.makeInputRef(inputFields.get(1).getType, 1))
    project.add(rexBuilder.makeNullLiteral(inputFields.get(2).getType))
    expandProjects.add(project)

    val project2: util.List[RexNode] = new util.ArrayList[RexNode]()
    project2.add(rexBuilder.makeInputRef(inputFields.get(0).getType, 0))
    project2.add(rexBuilder.makeBigintLiteral(new JBigDecimal(1)))
    expandProjects.add(project2)

    new LogicalExpand(cluster,  logicalTableScan.getTraitSet, logicalTableScan, expandProjects, 2)
  }

  @Test(expected = classOf[AssertionError])
  def testUnsupportedExpandProjectsWithRexCall(): Unit = {
    val inputFields = logicalTableScan.getRowType.getFieldList
    val expandProjects: util.List[util.List[RexNode]] = new util.ArrayList()
    val rexBuilder = logicalTableScan.getCluster.getRexBuilder

    val project: util.List[RexNode] = new util.ArrayList[RexNode]()
    project.add(rexBuilder.makeInputRef(inputFields.get(0).getType, 0))
    project.add(rexBuilder.makeInputRef(inputFields.get(1).getType, 1))
    project.add(rexBuilder.makeNullLiteral(inputFields.get(2).getType))
    expandProjects.add(project)

    val project2: util.List[RexNode] = new util.ArrayList[RexNode]()
    project2.add(rexBuilder.makeInputRef(inputFields.get(0).getType, 0))
    project2.add(
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        new RexInputRef(0, logicalTableScan.getRowType),
        new RexInputRef(1, logicalTableScan.getRowType)))
    project2.add(rexBuilder.makeBigintLiteral(new JBigDecimal(1)))
    expandProjects.add(project2)

    new LogicalExpand(cluster,  logicalTableScan.getTraitSet, logicalTableScan, expandProjects, 2)
  }

  @Test(expected = classOf[AssertionError])
  def testUnsupportedExpandProjectsWithNoCommonTypes(): Unit = {
    val inputFields = logicalTableScan.getRowType.getFieldList
    val expandProjects: util.List[util.List[RexNode]] = new util.ArrayList()
    val rexBuilder = logicalTableScan.getCluster.getRexBuilder

    val project: util.List[RexNode] = new util.ArrayList[RexNode]()
    project.add(rexBuilder.makeInputRef(inputFields.get(0).getType, 0))
    project.add(rexBuilder.makeBinaryLiteral(ByteString.of("10001", 2)))
    project.add(rexBuilder.makeNullLiteral(inputFields.get(2).getType))
    expandProjects.add(project)

    val project2: util.List[RexNode] = new util.ArrayList[RexNode]()
    project2.add(rexBuilder.makeInputRef(inputFields.get(0).getType, 0))
    project2.add(rexBuilder.makeTimestampLiteral(new TimestampString("2021-03-23 23:11:17.123"), 3))
    project2.add(rexBuilder.makeBigintLiteral(new JBigDecimal(1)))
    expandProjects.add(project2)

    new LogicalExpand(cluster,  logicalTableScan.getTraitSet, logicalTableScan, expandProjects, 2)
  }
}
