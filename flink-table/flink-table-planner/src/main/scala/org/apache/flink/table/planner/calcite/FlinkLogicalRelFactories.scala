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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.planner.calcite.FlinkRelFactories.{ExpandFactory, RankFactory}
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.runtime.operators.rank.{RankRange, RankType}

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.plan.{Contexts, RelOptCluster, RelOptTable}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.RelFactories._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlKind.{EXCEPT, INTERSECT, UNION}
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
  * Contains factory interface and default implementation for creating various
  * flink logical rel nodes.
  */
object FlinkLogicalRelFactories {

  val FLINK_LOGICAL_PROJECT_FACTORY = new ProjectFactoryImpl
  val FLINK_LOGICAL_FILTER_FACTORY = new FilterFactoryImpl
  val FLINK_LOGICAL_JOIN_FACTORY = new JoinFactoryImpl
  val FLINK_LOGICAL_CORRELATE_FACTORY = new CorrelateFactoryImpl
  val FLINK_LOGICAL_SORT_FACTORY = new SortFactoryImpl
  val FLINK_LOGICAL_AGGREGATE_FACTORY = new AggregateFactoryImpl
  val FLINK_LOGICAL_SET_OP_FACTORY = new SetOpFactoryImpl
  val FLINK_LOGICAL_VALUES_FACTORY = new ValuesFactoryImpl
  val FLINK_LOGICAL_TABLE_SCAN_FACTORY = new TableScanFactoryImpl
  val FLINK_LOGICAL_EXPAND_FACTORY = new ExpandFactoryImpl
  val FLINK_LOGICAL_RANK_FACTORY = new RankFactoryImpl

  /** A [[RelBuilderFactory]] that creates a [[RelBuilder]] that will
    * create logical relational expressions for everything. */
  val FLINK_LOGICAL_REL_BUILDER: RelBuilderFactory = FlinkRelBuilder.proto(
    Contexts.of(
      FLINK_LOGICAL_PROJECT_FACTORY,
      FLINK_LOGICAL_FILTER_FACTORY,
      FLINK_LOGICAL_JOIN_FACTORY,
      FLINK_LOGICAL_SORT_FACTORY,
      FLINK_LOGICAL_AGGREGATE_FACTORY,
      FLINK_LOGICAL_SET_OP_FACTORY,
      FLINK_LOGICAL_VALUES_FACTORY,
      FLINK_LOGICAL_TABLE_SCAN_FACTORY,
      FLINK_LOGICAL_EXPAND_FACTORY,
      FLINK_LOGICAL_RANK_FACTORY))

  /**
    * Implementation of [[ProjectFactory]] that returns a [[FlinkLogicalCalc]].
    */
  class ProjectFactoryImpl extends ProjectFactory {
    def createProject(
        input: RelNode,
        hints: util.List[RelHint],
        childExprs: util.List[_ <: RexNode],
        fieldNames: util.List[String]): RelNode = {
      val rexBuilder = input.getCluster.getRexBuilder
      val inputRowType = input.getRowType
      val programBuilder = new RexProgramBuilder(inputRowType, rexBuilder)
      childExprs.zip(fieldNames).foreach {
        case (childExpr, fieldName) => programBuilder.addProject(childExpr, fieldName)
      }
      val program = programBuilder.getProgram
      FlinkLogicalCalc.create(input, program)
    }
  }

  /**
    * Implementation of [[SortFactory]] that returns a [[FlinkLogicalSort]].
    */
  class SortFactoryImpl extends SortFactory {
    def createSort(
        input: RelNode,
        collation: RelCollation,
        offset: RexNode,
        fetch: RexNode): RelNode = {
      FlinkLogicalSort.create(input, collation, offset, fetch)
    }
  }

  /**
    * Implementation of [[SetOpFactory]] that
    * returns a flink [[org.apache.calcite.rel.core.SetOp]] for the particular kind of set
    * operation (UNION, EXCEPT, INTERSECT).
    */
  class SetOpFactoryImpl extends SetOpFactory {
    def createSetOp(kind: SqlKind, inputs: util.List[RelNode], all: Boolean): RelNode = {
      kind match {
        case UNION =>
          FlinkLogicalUnion.create(inputs, all)
        case EXCEPT =>
          FlinkLogicalMinus.create(inputs, all)
        case INTERSECT =>
          FlinkLogicalIntersect.create(inputs, all)
        case _ =>
          throw new AssertionError("not a set op: " + kind)
      }
    }
  }

  /**
    * Implementation of [[AggregateFactory]] that returns a [[FlinkLogicalAggregate]].
    */
  class AggregateFactoryImpl extends AggregateFactory {
    def createAggregate(
        input: RelNode,
        hints: util.List[RelHint],
        groupSet: ImmutableBitSet,
        groupSets: ImmutableList[ImmutableBitSet],
        aggCalls: util.List[AggregateCall]): RelNode = {
      FlinkLogicalAggregate.create(input, groupSet, groupSets, aggCalls)
    }
  }

  /**
    * Implementation of [[FilterFactory]] that returns a [[FlinkLogicalCalc]].
    */
  class FilterFactoryImpl extends FilterFactory {
    override def createFilter(
        input: RelNode,
        condition: RexNode,
        variablesSet: util.Set[CorrelationId]): RelNode = {
      // Create a program containing a filter.
      // Ignore the variablesSet for current implementation.
      val rexBuilder = input.getCluster.getRexBuilder
      val inputRowType = input.getRowType
      val programBuilder = new RexProgramBuilder(inputRowType, rexBuilder)
      programBuilder.addIdentity()
      programBuilder.addCondition(condition)
      val program = programBuilder.getProgram
      FlinkLogicalCalc.create(input, program)
    }
  }

  /**
    * Implementation of [[JoinFactory]] that returns a [[FlinkLogicalJoin]].
    */
  class JoinFactoryImpl extends JoinFactory {
    def createJoin(
        left: RelNode,
        right: RelNode,
        hints: util.List[RelHint],
        condition: RexNode,
        variablesSet: util.Set[CorrelationId],
        joinType: JoinRelType,
        semiJoinDone: Boolean): RelNode = {
      FlinkLogicalJoin.create(left, right, condition, joinType)
    }
  }

  /**
    * Implementation of [[CorrelateFactory]] that returns a [[FlinkLogicalCorrelate]].
    */
  class CorrelateFactoryImpl extends CorrelateFactory {
    def createCorrelate(
        left: RelNode,
        right: RelNode,
        correlationId: CorrelationId,
        requiredColumns: ImmutableBitSet,
        joinType: JoinRelType): RelNode = {
      FlinkLogicalCorrelate.create(left, right, correlationId, requiredColumns, joinType)
    }
  }

  /**
    * Implementation of [[ValuesFactory]] that returns a [[FlinkLogicalValues]].
    */
  class ValuesFactoryImpl extends ValuesFactory {
    def createValues(
        cluster: RelOptCluster,
        rowType: RelDataType,
        tuples: util.List[ImmutableList[RexLiteral]]): RelNode = {
      FlinkLogicalValues.create(
        cluster, null, rowType, ImmutableList.copyOf[ImmutableList[RexLiteral]](tuples))
    }
  }

  /**
    * Implementation of [[TableScanFactory]] that returns a
    * [[FlinkLogicalLegacyTableSourceScan]] or [[FlinkLogicalDataStreamTableScan]].
    */
  class TableScanFactoryImpl extends TableScanFactory {
    def createScan(toRelContext: ToRelContext, table: RelOptTable): RelNode = {
      val cluster = toRelContext.getCluster
      val hints = toRelContext.getTableHints
      val tableScan = LogicalTableScan.create(cluster, table, hints)
      tableScan match {
        case s: LogicalTableScan if FlinkLogicalLegacyTableSourceScan.isTableSourceScan(s) =>
          FlinkLogicalLegacyTableSourceScan.create(
            cluster,
            hints,
            s.getTable.asInstanceOf[FlinkPreparingTableBase])
        case s: LogicalTableScan if FlinkLogicalDataStreamTableScan.isDataStreamTableScan(s) =>
          FlinkLogicalDataStreamTableScan.create(
            cluster,
            hints,
            s.getTable.asInstanceOf[FlinkPreparingTableBase])
      }
    }
  }

  /**
    * Implementation of [[FlinkRelFactories.ExpandFactory]] that returns a
    * [[FlinkLogicalExpand]].
    */
  class ExpandFactoryImpl extends ExpandFactory {
    def createExpand(
        input: RelNode,
        projects: util.List[util.List[RexNode]],
        expandIdIndex: Int): RelNode = {
      FlinkLogicalExpand.create(input, projects, expandIdIndex)
    }
  }

  /**
    * Implementation of [[FlinkRelFactories.RankFactory]] that returns a
    * [[FlinkLogicalRank]].
    */
  class RankFactoryImpl extends RankFactory {
    def createRank(
        input: RelNode,
        partitionKey: ImmutableBitSet,
        orderKey: RelCollation,
        rankType: RankType,
        rankRange: RankRange,
        rankNumberType: RelDataTypeField,
        outputRankNumber: Boolean): RelNode = {
      FlinkLogicalRank.create(input, partitionKey, orderKey, rankType, rankRange,
        rankNumberType, outputRankNumber)
    }
  }
}
