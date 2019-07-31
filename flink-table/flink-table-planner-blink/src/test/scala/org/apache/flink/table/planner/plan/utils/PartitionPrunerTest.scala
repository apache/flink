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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.{DataTypes, TableConfig}
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.junit.Assert.assertEquals
import org.junit.Test

import java.math.BigDecimal
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

/**
  * Test for [[PartitionPruner]].
  */
class PartitionPrunerTest extends RexNodeTestBase {

  @Test
  def testPrunePartitions(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 0)
    // 100
    val t1 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    val c1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t0, t1)
    // name
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(0), 1)
    // 'test%'
    val t3 = rexBuilder.makeLiteral("test%")
    val c2 = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, t2, t3)
    // amount > 100 and name like 'test%'
    val c3 = rexBuilder.makeCall(SqlStdOperatorTable.AND, c1, c2)

    val partitionFieldNames = Array("amount", "name", "flag")
    val partitionFieldTypes = Array(DataTypes.INT().getLogicalType,
      DataTypes.VARCHAR(100).getLogicalType, DataTypes.BOOLEAN().getLogicalType)

    val allPartitions: JList[JMap[String, String]] = List(
      Map("amount" -> "20", "name" -> "test1", "flag" -> "true").asJava,
      Map("amount" -> "150", "name" -> "test2", "flag" -> "false").asJava,
      Map("amount" -> "200", "name" -> "Test3", "flag" -> "false").asJava
    ).asJava

    val config = new TableConfig
    val prunedPartitions = PartitionPruner.prunePartitions(
      config,
      partitionFieldNames,
      partitionFieldTypes,
      allPartitions,
      c3
    )

    assertEquals(1, prunedPartitions.size())
    assertEquals("150", prunedPartitions.get(0).get("amount"))
  }

  @Test
  def testPrunePartitionsWithUdf(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 1)
    // MyUdf(amount)
    val t1 = rexBuilder.makeCall(new ScalarSqlFunction("MyUdf", "MyUdf", Func1, typeFactory), t0)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    // MyUdf(amount) > 100
    val c = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t2)

    val partitionFieldNames = Array("name", "amount")
    val partitionFieldTypes = Array(
      DataTypes.VARCHAR(100).getLogicalType, DataTypes.INT().getLogicalType)

    val allPartitions: JList[JMap[String, String]] = List(
      Map("amount" -> "20", "name" -> "test1").asJava,
      Map("amount" -> "150", "name" -> "test2").asJava,
      Map("amount" -> "200", "name" -> "Test3").asJava
    ).asJava

    val config = new TableConfig
    val prunedPartitions = PartitionPruner.prunePartitions(
      config,
      partitionFieldNames,
      partitionFieldTypes,
      allPartitions,
      c
    )

    assertEquals(2, prunedPartitions.size())
    assertEquals("150", prunedPartitions.get(0).get("amount"))
    assertEquals("200", prunedPartitions.get(1).get("amount"))
  }

}
