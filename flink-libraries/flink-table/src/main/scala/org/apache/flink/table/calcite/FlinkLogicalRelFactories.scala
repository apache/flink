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

package org.apache.flink.table.calcite

import java.lang.{String => JString}
import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{Contexts, RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rel.core.RelFactories._
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex._
import org.apache.calcite.sql.{SemiJoinType, SqlKind}
import org.apache.calcite.sql.SqlKind.{EXCEPT, INTERSECT, UNION}
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.api.QueryConfig
import org.apache.flink.table.calcite.FlinkRelFactories.{ExpandFactory, SinkFactory}
import org.apache.flink.table.plan.nodes.calcite.LogicalExpand
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.sinks.TableSink

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
  val FLINK_LOGICAL_SEMI_JOIN_FACTORY = new SemiJoinFactoryImpl
  val FLINK_LOGICAL_SORT_FACTORY = new SortFactoryImpl
  val FLINK_LOGICAL_AGGREGATE_FACTORY = new AggregateFactoryImpl
  val FLINK_LOGICAL_MATCH_FACTORY = new MatchFactoryImpl
  val FLINK_LOGICAL_SET_OP_FACTORY = new SetOpFactoryImpl
  val FLINK_LOGICAL_VALUES_FACTORY = new ValuesFactoryImpl
  val FLINK_LOGICAL_TABLE_SCAN_FACTORY = new TableScanFactoryImpl
  val FLINK_LOGICAL_EXPAND_FACTORY = new ExpandFactoryImpl

  /** A [[RelBuilderFactory]] that creates a [[RelBuilder]] that will
    * create logical relational expressions for everything. */
  val FLINK_LOGICAL_REL_BUILDER: RelBuilderFactory = FlinkRelBuilder.proto(
    Contexts.of(
      FLINK_LOGICAL_PROJECT_FACTORY,
      FLINK_LOGICAL_FILTER_FACTORY,
      FLINK_LOGICAL_JOIN_FACTORY,
      FLINK_LOGICAL_SEMI_JOIN_FACTORY,
      FLINK_LOGICAL_SORT_FACTORY,
      FLINK_LOGICAL_AGGREGATE_FACTORY,
      FLINK_LOGICAL_MATCH_FACTORY,
      FLINK_LOGICAL_SET_OP_FACTORY,
      FLINK_LOGICAL_VALUES_FACTORY,
      FLINK_LOGICAL_TABLE_SCAN_FACTORY,
      FLINK_LOGICAL_EXPAND_FACTORY))

  /**
    * Implementation of [[ProjectFactory]] that returns a [[FlinkLogicalCalc]].
    */
  class ProjectFactoryImpl extends ProjectFactory {
    def createProject(
        input: RelNode,
        childExprs: util.List[_ <: RexNode],
        fieldNames: util.List[JString]): RelNode = {
      val rexBuilder = input.getCluster.getRexBuilder
      val inputRowType = input.getRowType
      val programBuilder = new RexProgramBuilder(inputRowType, rexBuilder)
      childExprs.zip(fieldNames).foreach { case (childExpr, fieldName) =>
        programBuilder.addProject(childExpr, fieldName)
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

    @deprecated // to be removed before 2.0
    def createSort(
        traits: RelTraitSet, input: RelNode,
        collation: RelCollation, offset: RexNode, fetch: RexNode): RelNode = {
      createSort(input, collation, offset, fetch)
    }
  }

  /**
    * Implementation of [[SetOpFactory]] that
    * returns a [[org.apache.calcite.rel.core.SetOp]] for the particular kind of set
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
    @SuppressWarnings(Array("deprecation"))
    def createAggregate(
        input: RelNode,
        indicator: Boolean,
        groupSet: ImmutableBitSet,
        groupSets: ImmutableList[ImmutableBitSet],
        aggCalls: util.List[AggregateCall]): RelNode = {
      FlinkLogicalAggregate.create(input, indicator, groupSet, groupSets, aggCalls)
    }
  }

  /**
    * Implementation of [[FilterFactory]] that returns a [[FlinkLogicalCalc]].
    */
  class FilterFactoryImpl extends FilterFactory {
    def createFilter(input: RelNode, condition: RexNode): RelNode = {
      // Create a program containing a filter.
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
    * Implementation of [[JoinFactory]] that returns a vanilla
    * [[org.apache.calcite.rel.logical.LogicalJoin]].
    */
  class JoinFactoryImpl extends JoinFactory {
    def createJoin(
        left: RelNode,
        right: RelNode,
        condition: RexNode,
        variablesSet: util.Set[CorrelationId],
        joinType: JoinRelType,
        semiJoinDone: Boolean): RelNode = {
      FlinkLogicalJoin.create(left, right, condition, joinType)
    }

    def createJoin(
        left: RelNode,
        right: RelNode,
        condition: RexNode,
        joinType: JoinRelType,
        variablesStopped: util.Set[JString],
        semiJoinDone: Boolean): RelNode = {
      createJoin(
        left,
        right,
        condition,
        CorrelationId.setOf(variablesStopped),
        joinType,
        semiJoinDone)
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
        joinType: SemiJoinType): RelNode = {
      FlinkLogicalCorrelate.create(left, right, correlationId, requiredColumns, joinType)
    }
  }

  /**
    * Implementation of [[SemiJoinFactory} that returns a [[FlinkLogicalSemiJoin]].
    */
  class SemiJoinFactoryImpl extends SemiJoinFactory {
    def createSemiJoin(left: RelNode, right: RelNode, condition: RexNode): RelNode = {
      val joinInfo: JoinInfo = JoinInfo.of(left, right, condition)
      FlinkLogicalSemiJoin.create(
        left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys, isAntiJoin = false)
    }

    def createAntiJoin(left: RelNode, right: RelNode, condition: RexNode): RelNode = {
      val joinInfo: JoinInfo = JoinInfo.of(left, right, condition)
      FlinkLogicalSemiJoin.create(
        left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys, isAntiJoin = true)
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
        cluster, rowType, ImmutableList.copyOf[ImmutableList[RexLiteral]](tuples))
    }
  }

  /**
    * Implementation of [[TableScanFactory]] that returns a
    * [[FlinkLogicalTableSourceScan]] or [[FlinkLogicalNativeTableScan]].
    */
  class TableScanFactoryImpl extends TableScanFactory {
    def createScan(cluster: RelOptCluster, table: RelOptTable): RelNode = {
      val tableScan = LogicalTableScan.create(cluster, table)
      tableScan match {
        case tss: LogicalTableScan if FlinkLogicalTableSourceScan.isLogicalTableSourceScan(tss) =>
          FlinkLogicalTableSourceScan.create(cluster, tss.getTable.asInstanceOf[FlinkRelOptTable])
        case nts: LogicalTableScan if FlinkLogicalNativeTableScan.isLogicalNativeTableScan(nts) =>
          FlinkLogicalNativeTableScan.create(cluster, nts.getTable.asInstanceOf[FlinkRelOptTable])
      }
    }
  }

  /**
    * Implementation of [[MatchFactory]] that returns a [[LogicalMatch]].
    */
  class MatchFactoryImpl extends MatchFactory {
    def createMatch(
        input: RelNode,
        pattern: RexNode,
        rowType: RelDataType,
        strictStart: Boolean,
        strictEnd: Boolean,
        patternDefinitions: util.Map[JString, RexNode],
        measures: util.Map[JString, RexNode],
        after: RexNode,
        subsets: util.Map[JString, _ <: util.SortedSet[JString]],
        rowsPerMatch: RexNode,
        partitionKeys: util.List[RexNode],
        orderKeys: RelCollation,
        interval: RexNode): RelNode = {
      FlinkLogicalMatch.create(
        input,
        pattern,
        strictStart,
        strictEnd,
        patternDefinitions,
        measures,
        after,
        subsets,
        rowsPerMatch,
        partitionKeys,
        orderKeys,
        interval,
        rowType)
    }
  }

  /**
    * Implementation of [[FlinkRelFactories#ExpandFactory]] that returns a
    * [[LogicalExpand]].
    */
  class ExpandFactoryImpl extends ExpandFactory {
    def createExpand(
        input: RelNode,
        rowType: RelDataType,
        projects: util.List[util.List[RexNode]],
        expandIdIndex: Int): RelNode = {
      FlinkLogicalExpand.create(input, rowType, projects, expandIdIndex)
    }
  }

  /**
    * Implementation of [[FlinkRelFactories#SinkFactory]] that returns a [[FlinkLogicalSink]].
    */
  class SinkFactoryImpl extends SinkFactory {
    def createSink(
      input: RelNode,
      sink: TableSink[_],
      sinkName: String): RelNode = {
      FlinkLogicalSink.create(input, sink, sinkName)
    }
  }
}
