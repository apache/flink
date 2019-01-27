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
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.codegen.{CodeGeneratorContext, CorrelateCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef, TraitSetHelper}
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.CorrelateUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelCollationTraitDef, RelDistribution, RelFieldCollation, RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexProgram}
import org.apache.calcite.sql.{SemiJoinType, SqlKind}
import org.apache.calcite.util.mapping.{Mapping, MappingType, Mappings}

import scala.collection.JavaConversions._

/**
  * Flink RelNode which matches along with join a user defined table function.
  */
class BatchExecCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    projectProgram: Option[RexProgram],
    val scan: FlinkLogicalTableFunctionScan,
    val condition: Option[RexNode],
    relRowType: RelDataType,
    joinType: SemiJoinType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, inputNode)
  with BatchPhysicalRel
  with RowBatchExecNode {

  require(joinType == SemiJoinType.INNER || joinType == SemiJoinType.LEFT)

  override def deriveRowType(): RelDataType = relRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    copy(traitSet, inputs.get(0), projectProgram, relRowType)
  }

  /**
    * Note: do not passing member 'child' because singleRel.replaceInput may update 'input' rel.
    */
  def copy(
      traitSet: RelTraitSet,
      child: RelNode,
      projectProgram: Option[RexProgram],
      outputType: RelDataType): RelNode = {
    new BatchExecCorrelate(
      cluster,
      traitSet,
      child,
      projectProgram,
      scan,
      condition,
      outputType,
      joinType,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
      .item("invocation", scan.getCall)
      .item("correlate", CorrelateUtil.correlateToString(
        input.getRowType, rexCall, sqlFunction, getExpressionString))
      .item("select", CorrelateUtil.selectToString(relRowType))
      .item("rowType", relRowType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

  override def isDeterministic: Boolean = CorrelateUtil.isDeterministic(scan, condition)

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    // Does not push broadcast distribution trait down into Correlate.
    if (requiredDistribution.getType == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      return null
    }

    def getOutputInputMapping: Mapping = {
      val inputFieldCnt = getInput.getRowType.getFieldCount
      projectProgram match {
        case Some(program) =>
          val projects = program.getProjectList.map(program.expandLocalRef)
          val mapping = Mappings.create(MappingType.INVERSE_FUNCTION, inputFieldCnt, projects.size)
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
        case _ =>
          val mapping = Mappings.create(MappingType.FUNCTION, inputFieldCnt, inputFieldCnt)
          (0 until inputFieldCnt).foreach {
            index => mapping.set(index, index)
          }
          mapping
      }
    }

    val mapping = getOutputInputMapping
    val appliedDistribution = requiredDistribution.apply(mapping)
    // If both distribution and collation can be pushed down, push them both. If only distribution
    // can be pushed down, only push down distribution. There is no possibility to only push down
    // collation here except for there is no distribution requirement.
    if ((!requiredDistribution.isTop) && (appliedDistribution eq FlinkRelDistribution.ANY)) {
      return null
    }
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val appliedCollation = TraitSetHelper.apply(requiredCollation, mapping)
    // push down collation if field collations are not empty
    // and the direction of each field collation is non-STRICTLY
    val canCollationPushedDown = appliedCollation.getFieldCollations.nonEmpty &&
      !appliedCollation.getFieldCollations.exists { c =>
        (c.getDirection eq RelFieldCollation.Direction.STRICTLY_ASCENDING) ||
          (c.getDirection eq RelFieldCollation.Direction.STRICTLY_DESCENDING)
      }
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
    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val operatorCtx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
    val transformation = CorrelateCodeGenerator.generateCorrelateTransformation(
      tableEnv,
      operatorCtx,
      inputTransformation,
      input.getRowType,
      projectProgram,
      scan,
      condition,
      relRowType,
      joinType,
      getResource.getParallelism,
      retainHeader = false,
      getExpressionString,
      ruleDescription)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

}
