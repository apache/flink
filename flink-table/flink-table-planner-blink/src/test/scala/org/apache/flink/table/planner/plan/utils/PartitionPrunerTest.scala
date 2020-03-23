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
import org.apache.flink.table.functions.FunctionIdentifier
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction

import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName.DATE
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.{DateString, TimeString, TimestampString}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.math.BigDecimal
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConversions._
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
    val t1 = rexBuilder.makeCall(new ScalarSqlFunction(
      FunctionIdentifier.of("MyUdf"),
      "MyUdf",
      Func1,
      typeFactory),
      t0)
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

  @Test
  def testTimePrunePartitions(): Unit = {
    val f0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0)
    val f1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIME, 0), 1)
    val f2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), 2)
    val f3 = rexBuilder.makeInputRef(
      typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3), 3)

    val c0 = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      f0,
      rexBuilder.makeDateLiteral(new DateString("2018-08-06")))

    val c1 = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      f1,
      rexBuilder.makeTimeLiteral(new TimeString("12:08:06"), 0))

    val c2 = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      f2,
      rexBuilder.makeTimestampLiteral(new TimestampString("2018-08-06 12:08:06.123"), 3))

    val c3 = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      f3,
      rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
        new TimestampString("2018-08-06 12:08:06.123"), 3))

    val condition = RexUtil.composeConjunction(rexBuilder, Seq(c0, c1, c2, c3))

    val partitionFieldNames = Array("f0", "f1", "f2", "f3")
    val partitionFieldTypes = Array(
      DataTypes.DATE().getLogicalType,
      DataTypes.TIME(0).getLogicalType,
      DataTypes.TIMESTAMP(3).getLogicalType,
      DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).getLogicalType)

    val allPartitions: JList[JMap[String, String]] = List(
      Map(
        "f0" -> "2018-08-05",
        "f1" -> "12:08:07",
        "f2" -> "2018-08-06 12:08:06.124",
        "f3" -> "2018-08-06 12:08:06.124").asJava,
      Map(
        "f0" -> "2018-08-07",
        "f1" -> "12:08:05",
        "f2" -> "2018-08-06 12:08:06.124",
        "f3" -> "2018-08-06 12:08:06.124").asJava,
      Map(
        "f0" -> "2018-08-07",
        "f1" -> "12:08:07",
        "f2" -> "2018-08-06 12:08:06.124",
        "f3" -> "2018-08-06 12:08:06.124").asJava
    ).asJava

    val config = new TableConfig
    val prunedPartitions = PartitionPruner.prunePartitions(
      config,
      partitionFieldNames,
      partitionFieldTypes,
      allPartitions,
      condition
    )

    assertEquals(1, prunedPartitions.size())
    assertEquals(allPartitions(2), prunedPartitions(0))
  }

}
