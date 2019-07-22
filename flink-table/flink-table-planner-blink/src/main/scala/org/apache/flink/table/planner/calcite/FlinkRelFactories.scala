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

import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalExpand, LogicalRank, LogicalSink}
import org.apache.flink.table.runtime.operators.rank.{RankRange, RankType}
import org.apache.flink.table.sinks.TableSink

import org.apache.calcite.plan.Contexts
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet

import java.util

/**
  * Contains factory interface and default implementation for creating various rel nodes.
  */
object FlinkRelFactories {

  val FLINK_REL_BUILDER: RelBuilderFactory = FlinkRelBuilder.proto(Contexts.empty)

  val DEFAULT_EXPAND_FACTORY = new ExpandFactoryImpl

  val DEFAULT_RANK_FACTORY = new RankFactoryImpl

  val DEFAULT_SINK_FACTORY = new SinkFactoryImpl

  /**
    * Can create a [[LogicalExpand]] of the
    * appropriate type for this rule's calling convention.
    */
  trait ExpandFactory {
    def createExpand(
        input: RelNode,
        rowType: RelDataType,
        projects: util.List[util.List[RexNode]],
        expandIdIndex: Int): RelNode
  }

  /**
    * Implementation of [[ExpandFactory]] that returns a [[LogicalExpand]].
    */
  class ExpandFactoryImpl extends ExpandFactory {
    def createExpand(
        input: RelNode,
        rowType: RelDataType,
        projects: util.List[util.List[RexNode]],
        expandIdIndex: Int): RelNode = LogicalExpand.create(input, rowType, projects, expandIdIndex)
  }

  /**
    * Can create a [[LogicalRank]] of the
    * appropriate type for this rule's calling convention.
    */
  trait RankFactory {
    def createRank(
        input: RelNode,
        partitionKey: ImmutableBitSet,
        orderKey: RelCollation,
        rankType: RankType,
        rankRange: RankRange,
        rankNumberType: RelDataTypeField,
        outputRankNumber: Boolean): RelNode
  }

  /**
    * Implementation of [[RankFactory]] that returns a [[LogicalRank]].
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
      LogicalRank.create(input, partitionKey, orderKey, rankType, rankRange,
        rankNumberType, outputRankNumber)
    }
  }

  /**
    * Can create a [[LogicalSink]] of the
    * appropriate type for this rule's calling convention.
    */
  trait SinkFactory {

    def createSink(
        input: RelNode,
        sink: TableSink[_],
        sinkName: String): RelNode
  }

  /**
    * Implementation of [[SinkFactory]] that returns a [[LogicalSink]].
    */
  class SinkFactoryImpl extends SinkFactory {

    def createSink(
        input: RelNode,
        sink: TableSink[_],
        sinkName: String): RelNode = LogicalSink.create(input, sink, sinkName)
  }
}
