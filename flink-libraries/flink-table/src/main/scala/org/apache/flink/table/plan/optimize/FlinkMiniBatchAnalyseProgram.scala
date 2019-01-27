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
package org.apache.flink.table.plan.optimize

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions.ExpressionUtils.{isRowtimeAttribute, isTimeIntervalLiteral}
import org.apache.flink.table.plan.logical.{SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.rules.physical.stream.MiniBatchAssignerRules

import scala.collection.JavaConverters._

/**
  * A FlinkMiniBatchAnalyseProgram analyses MiniBatch configs through the entire rel tree, to
  * determine whether the to enable MiniBatch and the size of MiniBatch.
  */
class FlinkMiniBatchAnalyseProgram[OC <: OptimizeContext] extends FlinkOptimizeProgram[OC] {

  override def optimize(input: RelNode, context: OC): RelNode = {
    val config = context.getContext.unwrap(classOf[TableConfig])
    if (config.getConf.contains(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY) &&
      config.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY) > 0) {
      // step1: validation
      val validation = new MiniBatchValidation
      validation.go(input)

      // step2: add minibatch assigner node
      val hepProgram = FlinkHepRuleSetProgramBuilder
        .newBuilder[OC]
        .add(RuleSets.ofList(
          MiniBatchAssignerRules.UNARY,
          MiniBatchAssignerRules.BINARY,
          MiniBatchAssignerRules.UNION))
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.TOP_DOWN)
        .build()

      hepProgram.optimize(input, context)
    } else {
      input
    }
  }

  /** The MiniBatchValidation will check whether Event-Time Node is in the query, otherwise
    * throw an unsupported exception.
    * In the future, we will support MiniBatch for EventTime Node */
  class MiniBatchValidation extends RelVisitor {
    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case _: StreamExecWatermarkAssigner =>
          throw new TableException("MiniBatch is not supported when watermark is defined.")

        case w: StreamExecGroupWindowAggregate =>
          val isEventTime = w.window match {
            case TumblingGroupWindow(_, timeField, size)
              if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size) => true
            case SlidingGroupWindow(_, timeField, size, _)
              if isRowtimeAttribute(timeField) && isTimeIntervalLiteral(size) => true
            case SessionGroupWindow(_, timeField, _)
              if isRowtimeAttribute(timeField) => true
            case _ => false
          }
          if (isEventTime) {
            throw new TableException("MiniBatch is not supported when Window Aggregate is used.")
          }

        case m: StreamExecMatch =>
          val rowtimeFields = m.getInput.getRowType
            .getFieldList.asScala
            .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
          // when the event-time field is set, the CEP node is event-time-mode
          if (rowtimeFields.nonEmpty) {
            throw new TableException("MiniBatch is not supported when Event-Time CEP is used.")
          }

        case wj: StreamExecWindowJoin =>
          if (wj.isRowTime) {
            throw new TableException(
              "MiniBatch is not supported when Event-Time WindowedJoin is used.")
          }

        case _ =>
          super.visit(node, ordinal, parent)
      }
    }
  }
}
