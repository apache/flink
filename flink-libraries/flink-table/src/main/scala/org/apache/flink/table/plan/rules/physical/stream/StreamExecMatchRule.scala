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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.logical.MatchRecognize
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalMatch
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecMatch
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.{MatchUtil, RexDefaultVisitor}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlAggFunction

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StreamExecMatchRule
  extends ConverterRule(
    classOf[FlinkLogicalMatch],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecMatchRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val logicalMatch: FlinkLogicalMatch = call.rel(0).asInstanceOf[FlinkLogicalMatch]

    validateAggregations(logicalMatch.getMeasures.values().asScala)
    validateAggregations(logicalMatch.getPatternDefinitions.values().asScala)
    true
  }

  override def convert(rel: RelNode): RelNode = {
    val logicalMatch: FlinkLogicalMatch = rel.asInstanceOf[FlinkLogicalMatch]

    val logicalKeys = logicalMatch.getPartitionKeys.asScala.map {
      case inputRef: RexInputRef => inputRef.getIndex
    }

    val requiredDistribution = if (logicalKeys.nonEmpty) {
      FlinkRelDistribution.hash(logicalKeys.map(Integer.valueOf))
    } else {
      FlinkRelDistribution.ANY
    }
    val requiredTraitSet = logicalMatch.getInput.getTraitSet.replace(
      FlinkConventions.STREAM_PHYSICAL).replace(requiredDistribution)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val convertInput: RelNode =
      RelOptRule.convert(logicalMatch.getInput, requiredTraitSet)

    try {
      Class
        .forName("org.apache.flink.cep.pattern.Pattern",
          false,
          Thread.currentThread().getContextClassLoader)
    } catch {
      case ex: ClassNotFoundException => throw new TableException(
        "MATCH RECOGNIZE clause requires flink-cep dependency to be present on the classpath.", ex)
    }

    new StreamExecMatch(
      rel.getCluster,
      providedTraitSet,
      convertInput,
      MatchRecognize(
        logicalMatch.getPattern,
        logicalMatch.isStrictStart,
        logicalMatch.isStrictEnd,
        logicalMatch.getPatternDefinitions,
        logicalMatch.getMeasures,
        logicalMatch.getAfter,
        logicalMatch.getSubsets,
        logicalMatch.getRowsPerMatch,
        logicalMatch.getPartitionKeys,
        logicalMatch.getOrderKeys,
        logicalMatch.getInterval
      ),
      new BaseRowSchema(logicalMatch.getRowType),
      new BaseRowSchema(logicalMatch.getInput.getRowType))
  }

  private def validateAggregations(expr: Iterable[RexNode]): Unit = {
    val validator = new AggregationsValidator
    expr.foreach(_.accept(validator))
  }

  class AggregationsValidator extends RexDefaultVisitor[Object] {

    override def visitCall(call: RexCall): AnyRef = {
      call.getOperator match {
        case _: SqlAggFunction =>
          call.accept(new MatchUtil.AggregationPatternVariableFinder)
        case _ =>
          call.getOperands.asScala.foreach(_.accept(this))
      }

      null
    }

    override def visitNode(rexNode: RexNode): AnyRef = {
      null
    }
  }
}

object StreamExecMatchRule {
  val INSTANCE: RelOptRule = new StreamExecMatchRule
}
