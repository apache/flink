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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef, TraitSetHelper}
import org.apache.flink.table.plan.nodes.exec.{RowBatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.CalcUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexCall, RexInputRef, RexProgram}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.mapping.{Mapping, MappingType, Mappings}

import scala.collection.JavaConversions._

/**
  * Flink RelNode which matches along with LogicalCalc.
  */
class BatchExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowRelDataType: RelDataType,
    calcProgram: RexProgram,
    val ruleDescription: String)
  extends Calc(cluster, traitSet, input, calcProgram)
  with BatchPhysicalRel
  with RowBatchExecNode {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new BatchExecCalc(
      cluster,
      traitSet,
      child,
      getRowType,
      program,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("select", CalcUtil.selectionToString(calcProgram, getExpressionString))
      .itemIf("where", CalcUtil.conditionToString(calcProgram, getExpressionString),
        calcProgram.getCondition != null)
  }

  override def isDeterministic: Boolean = CalcUtil.isDeterministic(program)

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    FlinkLogicalCalc.computeCost(calcProgram, planner, metadata, this)
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    // Does not push broadcast distribution trait down into Calc.
    if (requiredDistribution.getType == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      return null
    }
    val projects = calcProgram.getProjectList.map(calcProgram.expandLocalRef)

    def getProjectMapping: Mapping = {
      val mapping = Mappings.create(MappingType.INVERSE_FUNCTION,
        getInput.getRowType.getFieldCount, projects.size)
      projects.zipWithIndex.foreach {
        case (project, index) =>
          project match {
            case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
            case call: RexCall if call.getKind == SqlKind.AS =>
              call.getOperands.head match {
                case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
                case _ => // ignore
              }
            case _ => // ignore
          }
      }
      mapping.inverse()
    }
    val mapping = getProjectMapping
    val appliedDistribution = requiredDistribution.apply(mapping)
    // If both distribution and collation can be pushed down, push them both. If only distribution
    // can be pushed down, only push down distribution. There is no possibility to only push down
    // collation here except for there is no distribution requirement.
    if ((!requiredDistribution.isTop) &&
        (appliedDistribution eq FlinkRelDistribution.ANY)) {
      return null
    }
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val appliedCollation = TraitSetHelper.apply(requiredCollation, mapping)
    val canCollationPushedDown = !appliedCollation.getFieldCollations.isEmpty
    // If required traits only contains collation requirements, but collation keys are not columns
    // from input, then no need to push down required traits.
    if ((appliedDistribution eq FlinkRelDistribution.ANY) && !canCollationPushedDown) {
      return null
    }
    var pushDownTraits = getInput.getTraitSet
    var providedTraits = getTraitSet
    if (!appliedDistribution.isTop) {
      pushDownTraits = pushDownTraits.replace(appliedDistribution)
      providedTraits = providedTraits.replace(requiredDistribution)
    }
    if (canCollationPushedDown) {
      pushDownTraits = pushDownTraits.replace(appliedCollation)
      providedTraits = providedTraits.replace(requiredCollation)
    }
    val newInput = RelOptRule.convert(getInput, pushDownTraits)
    copy(providedTraits, Seq(newInput))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val condition = if (calcProgram.getCondition != null) {
      Some(calcProgram.expandLocalRef(calcProgram.getCondition))
    } else {
      None
    }
    val ctx = CodeGeneratorContext(config, supportReference = true)
    val substituteStreamOperator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      cluster,
      input.getRowType,
      inputTransform,
      getRowType,
      config,
      calcProgram,
      condition,
      ruleDescription = ruleDescription
    )

    val transformation = new OneInputTransformation(
      inputTransform,
      CalcUtil.calcToString(calcProgram, getExpressionString),
      substituteStreamOperator,
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType),
      getResource.getParallelism)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

}
