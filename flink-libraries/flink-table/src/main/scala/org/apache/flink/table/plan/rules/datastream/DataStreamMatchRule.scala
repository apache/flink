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

package org.apache.flink.table.plan.rules.datastream

import java.util.{List => JList}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.plan.logical.MatchRecognize
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamMatch
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalMatch
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.RexDefaultVisitor

import scala.collection.JavaConverters._
import scala.collection.mutable

class DataStreamMatchRule
  extends ConverterRule(
    classOf[FlinkLogicalMatch],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamMatchRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val logicalMatch: FlinkLogicalMatch = call.rel(0).asInstanceOf[FlinkLogicalMatch]

    // This check might be obsolete once CALCITE-2747 is resolved
    validateAmbiguousColumns(logicalMatch)
    true
  }

  override def convert(rel: RelNode): RelNode = {
    val logicalMatch: FlinkLogicalMatch = rel.asInstanceOf[FlinkLogicalMatch]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convertInput: RelNode =
      RelOptRule.convert(logicalMatch.getInput, FlinkConventions.DATASTREAM)

    try {
      Class
        .forName("org.apache.flink.cep.pattern.Pattern",
          false,
          Thread.currentThread().getContextClassLoader)
    } catch {
      case ex: ClassNotFoundException => throw new TableException(
        "MATCH RECOGNIZE clause requires flink-cep dependency to be present on the classpath.", ex)
    }

    new DataStreamMatch(
      rel.getCluster,
      traitSet,
      convertInput,
      MatchRecognize(
        logicalMatch.getInput,
        logicalMatch.getRowType,
        logicalMatch.getPattern,
        logicalMatch.getPatternDefinitions,
        logicalMatch.getMeasures,
        logicalMatch.getAfter,
        logicalMatch.getSubsets,
        logicalMatch.isAllRows,
        logicalMatch.getPartitionKeys,
        logicalMatch.getOrderKeys,
        logicalMatch.getInterval
      ),
      new RowSchema(logicalMatch.getRowType),
      new RowSchema(logicalMatch.getInput.getRowType))
  }

  private def validateAmbiguousColumns(logicalMatch: FlinkLogicalMatch): Unit = {
    if (logicalMatch.isAllRows) {
      throw new TableException("All rows per match mode is not supported yet.")
    } else {
      val refNameFinder = new RefNameFinder(logicalMatch.getInput.getRowType)
      validateAmbiguousColumnsOnRowPerMatch(
        logicalMatch.getPartitionKeys,
        logicalMatch.getMeasures.keySet().asScala,
        logicalMatch.getRowType,
        refNameFinder)
    }
  }

  private def validateAmbiguousColumnsOnRowPerMatch(
      partitionKeys: JList[RexNode],
      measuresNames: mutable.Set[String],
      expectedSchema: RelDataType,
      refNameFinder: RefNameFinder)
    : Unit = {
    val actualSize = partitionKeys.size() + measuresNames.size
    val expectedSize = expectedSchema.getFieldCount
    if (actualSize != expectedSize) {
      //try to find ambiguous column

      val ambiguousColumns = partitionKeys.asScala.map(_.accept(refNameFinder))
        .filter(measuresNames.contains).mkString("{", ", ", "}")

      throw new ValidationException(s"Columns ambiguously defined: $ambiguousColumns")
    }
  }

  private class RefNameFinder(inputSchema: RelDataType) extends RexDefaultVisitor[String] {

    override def visitInputRef(inputRef: RexInputRef): String = {
      inputSchema.getFieldList.get(inputRef.getIndex).getName
    }

    override def visitNode(rexNode: RexNode): String =
      throw new TableException(s"PARTITION BY clause accepts only input reference. Found $rexNode")
  }
}

object DataStreamMatchRule {
  val INSTANCE: RelOptRule = new DataStreamMatchRule
}
