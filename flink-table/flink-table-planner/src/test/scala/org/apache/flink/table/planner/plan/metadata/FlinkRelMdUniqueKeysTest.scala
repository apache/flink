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

import org.apache.flink.table.planner.plan.nodes.calcite.LogicalExpand
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.ExpandUtil

import com.google.common.collect.{ImmutableList, ImmutableSet}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.sql.`type`.SqlTypeName.VARCHAR
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{EQUALS, LESS_THAN}
import org.apache.calcite.util.ImmutableBitSet
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Collections

import scala.collection.JavaConversions._

class FlinkRelMdUniqueKeysTest extends FlinkRelMdHandlerTestBase {

  @Test
  def testGetUniqueKeysOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach {
      scan => assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(scan).toSet)
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach {
      scan => assertNull(mq.getUniqueKeys(scan))
    }

    val table = relBuilder.getRelOptSchema
      .asInstanceOf[CalciteCatalogReader]
      .getTable(Seq("projected_table_source_table"))
      .asInstanceOf[TableSourceTable]
    val tableSourceScan = new StreamPhysicalTableSourceScan(
      cluster,
      streamPhysicalTraits,
      Collections.emptyList[RelHint](),
      table)
    assertEquals(uniqueKeys(Array(0, 2)), mq.getUniqueKeys(tableSourceScan).toSet)
  }

  @Test
  def testGetUniqueKeysOnProjectedTableScanWithPartialCompositePrimaryKey(): Unit = {
    val table = relBuilder.getRelOptSchema
      .asInstanceOf[CalciteCatalogReader]
      .getTable(Seq("projected_table_source_table_with_partial_pk"))
      .asInstanceOf[TableSourceTable]
    val tableSourceScan = new StreamPhysicalTableSourceScan(
      cluster,
      streamPhysicalTraits,
      Collections.emptyList[RelHint](),
      table)
    assertNull(mq.getUniqueKeys(tableSourceScan))
  }

  @Test
  def testGetUniqueKeysOnValues(): Unit = {
    assertNull(mq.getUniqueKeys(logicalValues))
    assertNull(mq.getUniqueKeys(emptyValues))
  }

  @Test
  def testGetUniqueKeysOnProject(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalProject).toSet)

    relBuilder.push(studentLogicalScan)
    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1)
    )
    val project1 = relBuilder.project(exprs).build()
    // INT -> BIGINT is an injective cast, so position 2 is also a unique key
    assertEquals(uniqueKeys(Array(1), Array(2)), mq.getUniqueKeys(project1).toSet)
    assertEquals(uniqueKeys(Array(1), Array(2)), mq.getUniqueKeys(project1, true).toSet)
  }

  @Test
  def testGetUniqueKeysOnProjectWithInjectiveCastToString(): Unit = {
    // INT -> STRING is an injective cast (each distinct int maps to a distinct string).
    // When a unique key column is cast this way, the uniqueness is preserved.
    relBuilder.push(studentLogicalScan)

    val stringType = typeFactory.createSqlType(VARCHAR, 100)

    // Project: CAST(id AS STRING), name
    // id (position 0 in source) is the unique key
    val exprs = List(
      rexBuilder.makeCast(stringType, relBuilder.field(0)), // CAST(id AS STRING)
      relBuilder.field(1) // name
    )
    val project = relBuilder.project(exprs).build()

    // The casted id at position 0 should still be recognized as unique
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(project).toSet)
  }

  @Test
  def testGetUniqueKeysOnProjectWithMultipleKeyReferences(): Unit = {
    // When the same unique key column appears multiple times in a projection
    // (either raw or via injective cast), all references are recognized as keys.
    relBuilder.push(studentLogicalScan)

    val stringType = typeFactory.createSqlType(VARCHAR, 100)

    // Project: CAST(id AS STRING), id, name
    val exprs = List(
      rexBuilder.makeCast(stringType, relBuilder.field(0)), // CAST(id AS STRING) - injective
      relBuilder.field(0), // id (raw reference)
      relBuilder.field(1) // name
    )
    val project = relBuilder.project(exprs).build()

    // Both position 0 (STRING cast of id) and position 1 (raw id) are unique keys
    assertEquals(uniqueKeys(Array(0), Array(1)), mq.getUniqueKeys(project).toSet)
  }

  @Test
  def testGetUniqueKeysOnProjectInjectiveCastOnlyPreservesExistingKeys(): Unit = {
    // Injective casts PRESERVE uniqueness but don't CREATE it.
    // Casting a non-key column with an injective cast doesn't make it a key.
    relBuilder.push(studentLogicalScan)

    val stringType = typeFactory.createSqlType(VARCHAR, 100)

    // Project: id, CAST(name AS STRING)
    // id is the unique key; name is NOT a key (even after casting)
    val exprs = List(
      relBuilder.field(0), // id - the unique key
      rexBuilder.makeCast(stringType, relBuilder.field(1)) // CAST(name AS STRING) - not a key
    )
    val project = relBuilder.project(exprs).build()

    // Only position 0 (id) is a unique key
    // Position 1 (cast of name) is NOT a key because name wasn't a key to begin with
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(project).toSet)
  }

  @Test
  def testGetUniqueKeysOnProjectNonInjectiveCastLosesKey(): Unit = {
    // STRING -> INT is NOT an injective cast (e.g., "1" and "01" both become 1).
    // When a unique key is cast this way, the uniqueness cannot be guaranteed.
    relBuilder.push(studentLogicalScan)

    val stringType = typeFactory.createSqlType(VARCHAR, 100)

    // First, project id as STRING to simulate a STRING key column
    val stringKeyExprs = List(
      rexBuilder.makeCast(stringType, relBuilder.field(0)), // CAST(id AS STRING)
      relBuilder.field(1) // name
    )
    val stringKeyProject = relBuilder.project(stringKeyExprs).build()
    // At this point, position 0 is a STRING that's still a unique key
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(stringKeyProject).toSet)

    // Now cast the STRING back to INT - this is a non-injective (narrowing) cast
    relBuilder.push(stringKeyProject)
    val narrowedExprs = List(
      rexBuilder.makeCast(intType, relBuilder.field(0)), // CAST(string_id AS INT) - NOT injective
      relBuilder.field(1) // name
    )
    val narrowedProject = relBuilder.project(narrowedExprs).build()

    // The key is LOST because STRING->INT is not injective
    assertEquals(uniqueKeys(), mq.getUniqueKeys(narrowedProject).toSet)
  }

  @Test
  def testGetUniqueKeysOnFilter(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalFilter).toSet)
  }

  @Test
  def testGetUniqueKeysOnWatermark(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalWatermarkAssigner).toSet)
  }

  @Test
  def testGetUniqueKeysOnMiniBatchAssigner(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(streamMiniBatchAssigner).toSet)
  }

  @Test
  def testGetUniqueKeysOnCalc(): Unit = {
    relBuilder.push(studentLogicalScan)
    // id < 100
    val expr = relBuilder.call(LESS_THAN, relBuilder.field(0), relBuilder.literal(100))
    val calc1 = createLogicalCalc(
      studentLogicalScan,
      logicalProject.getRowType,
      logicalProject.getProjects,
      List(expr))
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(logicalCalc).toSet)

    // id=1, id, cast(id AS bigint not null), cast(id AS int), $1
    val exprs = List(
      relBuilder.call(EQUALS, relBuilder.field(0), relBuilder.literal(1)),
      relBuilder.field(0),
      rexBuilder.makeCast(longType, relBuilder.field(0)),
      rexBuilder.makeCast(intType, relBuilder.field(0)),
      relBuilder.field(1)
    )
    val rowType = relBuilder.project(exprs).build().getRowType
    val calc2 = createLogicalCalc(studentLogicalScan, rowType, exprs, List(expr))
    // INT -> BIGINT is an injective cast, so position 2 is also a unique key
    assertEquals(uniqueKeys(Array(1), Array(2)), mq.getUniqueKeys(calc2).toSet)
    assertEquals(uniqueKeys(Array(1), Array(2)), mq.getUniqueKeys(calc2, true).toSet)
  }

  @Test
  def testGetUniqueKeysOnExpand(): Unit = {
    Array(logicalExpand, flinkLogicalExpand, batchExpand, streamExpand).foreach {
      expand => assertEquals(uniqueKeys(Array(0, 7)), mq.getUniqueKeys(expand).toSet)
    }

    val expandProjects = ExpandUtil.createExpandProjects(
      studentLogicalScan.getCluster.getRexBuilder,
      studentLogicalScan.getRowType,
      ImmutableBitSet.of(0, 1, 2, 3),
      ImmutableList.of(
        ImmutableBitSet.of(0),
        ImmutableBitSet.of(1),
        ImmutableBitSet.of(2),
        ImmutableBitSet.of(3)),
      Array.empty[Integer]
    )
    val expand = new LogicalExpand(
      cluster,
      studentLogicalScan.getTraitSet,
      studentLogicalScan,
      expandProjects,
      7)
    assertNull(mq.getUniqueKeys(expand))
  }

  @Test
  def testGetUniqueKeysOnExchange(): Unit = {
    Array(batchExchange, streamExchange).foreach {
      exchange => assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(exchange).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnRank(): Unit = {
    Array(logicalRank, flinkLogicalRank, batchLocalRank, batchGlobalRank, streamRank).foreach {
      rank => assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(rank).toSet)
    }

    Array(logicalRowNumber, flinkLogicalRowNumber, streamRowNumber)
      .foreach {
        rank => assertEquals(uniqueKeys(Array(0), Array(7)), mq.getUniqueKeys(rank).toSet)
      }
  }

  @Test
  def testGetUniqueKeysOnSort(): Unit = {
    Array(
      logicalSort,
      flinkLogicalSort,
      batchSort,
      streamSort,
      logicalSortLimit,
      flinkLogicalSortLimit,
      batchSortLimit,
      streamSortLimit,
      batchGlobalSortLimit,
      batchLocalSortLimit,
      logicalLimit,
      flinkLogicalLimit,
      batchLimit,
      streamLimit
    ).foreach(sort => assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(sort).toSet))
  }

  @Test
  def testGetUniqueKeysOnStreamExecDeduplicate(): Unit = {
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(streamProcTimeDeduplicateFirstRow).toSet)
    assertEquals(uniqueKeys(Array(1, 2)), mq.getUniqueKeys(streamProcTimeDeduplicateLastRow).toSet)
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(streamRowTimeDeduplicateFirstRow).toSet)
    assertEquals(uniqueKeys(Array(1, 2)), mq.getUniqueKeys(streamRowTimeDeduplicateLastRow).toSet)
  }

  @Test
  def testGetUniqueKeysOnStreamExecChangelogNormalize(): Unit = {
    assertEquals(uniqueKeys(Array(1, 0)), mq.getUniqueKeys(streamChangelogNormalize).toSet)
  }

  @Test
  def testGetUniqueKeysOnStreamExecDropUpdateBefore(): Unit = {
    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(streamDropUpdateBefore).toSet)
  }

  @Test
  def testGetUniqueKeysOnAggregate(): Unit = {
    Array(
      logicalAgg,
      flinkLogicalAgg,
      batchGlobalAggWithLocal,
      batchGlobalAggWithoutLocal,
      streamGlobalAggWithLocal,
      streamGlobalAggWithoutLocal).foreach {
      agg => assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(agg).toSet)
    }
    assertNull(mq.getUniqueKeys(batchLocalAgg))
    assertNull(mq.getUniqueKeys(streamLocalAgg))

    Array(
      logicalAggWithAuxGroup,
      flinkLogicalAggWithAuxGroup,
      batchGlobalAggWithLocalWithAuxGroup,
      batchGlobalAggWithoutLocalWithAuxGroup).foreach {
      agg => assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(agg).toSet)
    }
    assertNull(mq.getUniqueKeys(batchLocalAggWithAuxGroup))
  }

  @Test
  def testGetUniqueKeysOnWindowAgg(): Unit = {
    Array(
      logicalWindowAgg,
      flinkLogicalWindowAgg,
      batchGlobalWindowAggWithoutLocalAgg,
      batchGlobalWindowAggWithLocalAgg).foreach {
      agg =>
        assertEquals(
          ImmutableSet.of(
            ImmutableBitSet.of(0, 1, 3),
            ImmutableBitSet.of(0, 1, 4),
            ImmutableBitSet.of(0, 1, 5),
            ImmutableBitSet.of(0, 1, 6)),
          mq.getUniqueKeys(agg))
    }
    assertNull(mq.getUniqueKeys(batchLocalWindowAgg))

    Array(
      logicalWindowAggWithAuxGroup,
      flinkLogicalWindowAggWithAuxGroup,
      batchGlobalWindowAggWithoutLocalAggWithAuxGroup,
      batchGlobalWindowAggWithLocalAggWithAuxGroup
    ).foreach {
      agg =>
        assertEquals(
          ImmutableSet.of(
            ImmutableBitSet.of(0, 3),
            ImmutableBitSet.of(0, 4),
            ImmutableBitSet.of(0, 5),
            ImmutableBitSet.of(0, 6)),
          mq.getUniqueKeys(agg))
    }
    assertNull(mq.getUniqueKeys(batchLocalWindowAggWithAuxGroup))
  }

  @Test
  def testGetUniqueKeysOnOverAgg(): Unit = {
    Array(flinkLogicalOverAgg, batchOverAgg).foreach {
      agg => assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(agg).toSet)
    }

    assertEquals(uniqueKeys(Array(0)), mq.getUniqueKeys(streamOverAgg).toSet)
  }

  @Test
  def testGetUniqueKeysOnJoin(): Unit = {
    assertEquals(
      uniqueKeys(Array(1), Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalInnerJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinOnRHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalInnerJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(
      uniqueKeys(Array(1), Array(1, 5), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalLeftJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinOnRHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalLeftJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(
      uniqueKeys(Array(5), Array(1, 5), Array(5, 6), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalRightJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinOnLHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalRightJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(
      uniqueKeys(Array(1, 5), Array(1, 5, 6)),
      mq.getUniqueKeys(logicalFullJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinNotOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinOnRHSUniqueKeys).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinWithoutEquiCond).toSet)
    assertEquals(uniqueKeys(), mq.getUniqueKeys(logicalFullJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalSemiJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalSemiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getUniqueKeys(logicalSemiJoinOnRHSUniqueKeys))
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalSemiJoinWithoutEquiCond).toSet)
    assertEquals(
      uniqueKeys(Array(1)),
      mq.getUniqueKeys(logicalSemiJoinWithEquiAndNonEquiCond).toSet)

    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalAntiJoinOnUniqueKeys).toSet)
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalAntiJoinNotOnUniqueKeys).toSet)
    assertNull(mq.getUniqueKeys(logicalAntiJoinOnRHSUniqueKeys))
    assertEquals(uniqueKeys(Array(1)), mq.getUniqueKeys(logicalAntiJoinWithoutEquiCond).toSet)
    assertEquals(
      uniqueKeys(Array(1)),
      mq.getUniqueKeys(logicalAntiJoinWithEquiAndNonEquiCond).toSet)
  }

  @Test
  def testGetUniqueKeysOnLookupJoin(): Unit = {
    Array(batchLookupJoin, streamLookupJoin).foreach {
      join => assertEquals(uniqueKeys(), mq.getUniqueKeys(join).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnLookupJoinWithPk(): Unit = {
    Array(batchLookupJoinWithPk, streamLookupJoinWithPk).foreach {
      join =>
        assertEquals(uniqueKeys(Array(7), Array(0, 7), Array(0)), mq.getUniqueKeys(join).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnLookupJoinNotContainsPk(): Unit = {
    Array(batchLookupJoinNotContainsPk, streamLookupJoinNotContainsPk).foreach {
      join => assertEquals(uniqueKeys(), mq.getUniqueKeys(join).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnSetOp(): Unit = {
    Array(logicalUnionAll, logicalIntersectAll, logicalMinusAll).foreach {
      setOp => assertEquals(uniqueKeys(), mq.getUniqueKeys(setOp).toSet)
    }

    Array(logicalUnion, logicalIntersect, logicalMinus).foreach {
      setOp => assertEquals(uniqueKeys(Array(0, 1, 2, 3, 4)), mq.getUniqueKeys(setOp).toSet)
    }
  }

  @Test
  def testGetUniqueKeysOnDefault(): Unit = {
    assertNull(mq.getUniqueKeys(testRel))
  }

  @Test
  def testGetUniqueKeysOnTableScanTable(): Unit = {
    assertEquals(
      uniqueKeys(Array(0, 1), Array(0, 1, 5)),
      mq.getUniqueKeys(logicalLeftJoinOnContainedUniqueKeys).toSet
    )
    assertEquals(
      uniqueKeys(Array(0, 1, 5)),
      mq.getUniqueKeys(logicalLeftJoinOnDisjointUniqueKeys).toSet
    )
    assertEquals(
      uniqueKeys(),
      mq.getUniqueKeys(logicalLeftJoinWithNoneKeyTableUniqueKeys).toSet
    )
  }

  private def uniqueKeys(keys: Array[Int]*): Set[ImmutableBitSet] = {
    keys.map(k => ImmutableBitSet.of(k: _*)).toSet
  }
}
