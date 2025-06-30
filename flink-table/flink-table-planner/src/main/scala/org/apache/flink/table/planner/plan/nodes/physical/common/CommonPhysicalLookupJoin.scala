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
package org.apache.flink.table.planner.plan.nodes.physical.common

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.catalog.{ObjectIdentifier, UniqueConstraint}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel
import org.apache.flink.table.planner.plan.schema.{IntermediateRelTable, LegacyTableSourceTable, TableSourceTable}
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, ExpressionFormat, InputRefVisitor, JoinTypeUtil, LookupJoinUtil, RelExplainUtil, TemporalJoinUtil}
import org.apache.flink.table.planner.plan.utils.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.{AsyncOptions, Constant, FieldRef, FunctionParam}
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil._
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.types.PlannerTypeUtils

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType, TableScan}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rex._
import org.apache.calcite.sql.{SqlExplainLevel, SqlKind}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.mapping.IntPair

import java.util
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Common abstract RelNode for temporal table join which shares most methods.
 *
 * For a lookup join query:
 *
 * <pre> SELECT T.id, T.content, D.age FROM T JOIN userTable FOR SYSTEM_TIME AS OF T.proctime AS D
 * ON T.content = concat(D.name, '!') AND D.age = 11 AND T.id = D.id WHERE D.name LIKE 'Jack%'
 * </pre>
 *
 * The LookupJoin physical node encapsulates the following RelNode tree:
 *
 * <pre> Join (l.name = r.name) / \ RelNode Calc (concat(name, "!") as name, name LIKE 'Jack%') \|
 * DimTable (lookup-keys: age=11, id=l.id) (age, id, name) </pre>
 *
 * The important member fields in LookupJoin: <ul> <li>allLookupKeys: [$0=11, $1=l.id] ($0 and $1 is
 * the indexes of age and id in dim table)</li> <li>remainingCondition: l.name=r.name</li> <ul>
 *
 * The workflow of lookup join:
 *
 * 1) lookup records dimension table using the lookup-keys <br> 2) project & filter on the lookup-ed
 * records <br> 3) join left input record and lookup-ed records <br> 4) only outputs the rows which
 * match to the remainingCondition <br>
 *
 * @param inputRel
 *   input rel node
 * @param calcOnTemporalTable
 *   the calc (projection&filter) after table scan before joining
 */
abstract class CommonPhysicalLookupJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    // TODO: refactor this into TableSourceTable, once legacy TableSource is removed
    val temporalTable: RelOptTable,
    val calcOnTemporalTable: Option[RexProgram],
    val joinInfo: JoinInfo,
    val joinType: JoinRelType,
    val lookupHint: Option[RelHint] = Option.empty[RelHint],
    val upsertMaterialize: Boolean = false,
    val enableLookupShuffle: Boolean = false,
    val preferCustomShuffle: Boolean = false)
  extends SingleRel(cluster, traitSet, inputRel)
  with FlinkRelNode {

  val allLookupKeys: Map[Int, FunctionParam] = {
    // join key pairs from left input field index to temporal table field index
    val joinKeyPairs: Array[IntPair] =
      TemporalJoinUtil.getTemporalTableJoinKeyPairs(joinInfo, calcOnTemporalTable)
    // all potential index keys, mapping from field index in table source to LookupKey
    analyzeLookupKeys(cluster.getRexBuilder, joinKeyPairs, calcOnTemporalTable)
  }
  // split join condition(except the lookup keys) into pre-filter(used to filter the left input
  // before lookup) and remaining parts(used to filter the joined records)
  val (finalPreFilterCondition, finalRemainingCondition) = splitJoinCondition(
    cluster.getRexBuilder,
    inputRel.getRowType,
    allLookupKeys.values.toList,
    joinInfo)

  if (containsPythonCall(joinInfo.getRemaining(cluster.getRexBuilder))) {
    throw new TableException(
      "Only inner join condition with equality predicates supports the " +
        "Python UDF taking the inputs from the left table and the right table at the same time, " +
        "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  lazy val isAsyncEnabled: Boolean = LookupJoinUtil.isAsyncLookup(
    temporalTable,
    allLookupKeys.keys.map(Int.box).toList.asJava,
    lookupHint.orNull,
    upsertMaterialize,
    preferCustomShuffle && !upsertMaterialize)

  lazy val retryOptions: Option[RetryLookupOptions] =
    Option.apply(LookupJoinUtil.RetryLookupOptions.fromJoinHint(lookupHint.orNull))

  lazy val inputChangelogMode: ChangelogMode = getInputChangelogMode(getInput)

  lazy val tableConfig: TableConfig = unwrapTableConfig(this);

  lazy val asyncOptions: Option[AsyncOptions] = if (isAsyncEnabled) {
    Option.apply(
      LookupJoinUtil.getMergedAsyncOptions(lookupHint.orNull, tableConfig, inputChangelogMode))
  } else {
    // do not create asyncOptions if async is not enabled
    Option.empty[AsyncOptions]
  }

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val rightType = if (calcOnTemporalTable.isDefined) {
      calcOnTemporalTable.get.getOutputRowType
    } else {
      temporalTable.getRowType
    }
    SqlValidatorUtil.deriveJoinRowType(
      inputRel.getRowType,
      rightType,
      joinType,
      flinkTypeFactory,
      null,
      Collections.emptyList[RelDataTypeField])
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputFieldNames = getInput.getRowType.getFieldNames.asScala.toArray
    val tableFieldNames = temporalTable.getRowType.getFieldNames
    val resultFieldNames = getRowType.getFieldNames.asScala.toArray
    val whereString = calcOnTemporalTable match {
      case Some(calc) =>
        RelExplainUtil.conditionToString(
          calc,
          getExpressionString,
          preferExpressionFormat(pw),
          convertToExpressionDetail(pw.getDetailLevel))
      case None => ""
    }
    val lookupKeys = allLookupKeys
      .map {
        case (tableField, fieldKey: FieldRef) =>
          s"${tableFieldNames.get(tableField)}=${inputFieldNames(fieldKey.index)}"
        case (tableField, constantKey: Constant) =>
          s"${tableFieldNames.get(tableField)}=${RelExplainUtil.literalToString(constantKey.literal)}"
      }
      .mkString(", ")
    val selection = calcOnTemporalTable match {
      case Some(calc) =>
        val rightSelect = RelExplainUtil.selectionToString(
          calc,
          getExpressionString,
          preferExpressionFormat(pw),
          convertToExpressionDetail(pw.getDetailLevel))
        inputFieldNames.mkString(", ") + ", " + rightSelect
      case None =>
        resultFieldNames.mkString(", ")
    }
    val tableIdentifier: ObjectIdentifier = temporalTable match {
      case t: TableSourceTable => t.contextResolvedTable.getIdentifier
      case t: LegacyTableSourceTable[_] => t.tableIdentifier
    }

    super
      .explainTerms(pw)
      .item("table", tableIdentifier.asSummaryString())
      .item("joinType", JoinTypeUtil.getFlinkJoinType(joinType))
      .item("lookup", lookupKeys)
      .itemIf("where", whereString, whereString.nonEmpty)
      .itemIf(
        "joinCondition",
        joinConditionToString(resultFieldNames, preferExpressionFormat(pw), pw.getDetailLevel),
        finalRemainingCondition.isDefined || finalPreFilterCondition.isDefined
      )
      .item("select", selection)
      .itemIf("upsertMaterialize", "true", upsertMaterialize)
      .itemIf("async", asyncOptions.getOrElse(""), asyncOptions.isDefined)
      .itemIf("shuffle", "true", enableLookupShuffle)
      .itemIf("retry", retryOptions.getOrElse(""), retryOptions.isDefined)
  }

  private def getInputChangelogMode(rel: RelNode): ChangelogMode = rel match {
    case streamPhysicalRel: StreamPhysicalRel =>
      ChangelogPlanUtils.getChangelogMode(streamPhysicalRel).getOrElse(ChangelogMode.insertOnly())
    case hepRelVertex: HepRelVertex =>
      // there are multi hep-programs in PHYSICAL_REWRITE phase, this would be invoked during
      // hep-optimization, so need to deal with HepRelVertex
      getInputChangelogMode(hepRelVertex.getCurrentRel)
    case _ => ChangelogMode.insertOnly()
  }

  /**
   * Splits the remaining condition in joinInfo into the pre-filter(used to filter the left input
   * before lookup) and remaining parts(used to filter the joined records).
   */
  private def splitJoinCondition(
      rexBuilder: RexBuilder,
      leftRelDataType: RelDataType,
      leftKeys: List[FunctionParam],
      joinInfo: JoinInfo): (Option[RexNode], Option[RexNode]) = {
    // indexes of left key fields
    val leftKeyIndexes =
      leftKeys
        .filter(k => k.isInstanceOf[FieldRef])
        .map(k => k.asInstanceOf[FieldRef].index)
    val joinPairs = joinInfo.pairs().asScala.toArray
    // right lookup key index of temporal table may be duplicated in joinPairs,
    // we should filter the key-pair by checking left key index.
    val remainingPairs = joinPairs.filter(p => !leftKeyIndexes.contains(p.source))
    val joinRowType = getRowType
    // convert remaining pairs to RexInputRef tuple for building SqlStdOperatorTable.EQUALS calls
    val remainingEquals = remainingPairs.map {
      p =>
        val leftFieldType = leftRelDataType.getFieldList.get(p.source).getType
        val leftInputRef = new RexInputRef(p.source, leftFieldType)
        val rightIndex = leftRelDataType.getFieldCount + p.target
        val rightFieldType = joinRowType.getFieldList.get(rightIndex).getType
        val rightInputRef = new RexInputRef(rightIndex, rightFieldType)
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftInputRef, rightInputRef)
    }
    if (joinType.generatesNullsOnRight) {
      // only extract pre-filter for left & full outer joins(otherwise the pre-filter will always be pushed down)
      val (leftLocal, remaining) =
        joinInfo.nonEquiConditions.asScala.partition {
          r =>
            {
              val inputRefs = new InputRefVisitor()
              r.accept(inputRefs)
              // if all input refs belong to left
              inputRefs.getFields.forall(idx => idx < inputRel.getRowType.getFieldCount)
            }
        }
      val remainingAnds = remainingEquals ++ remaining
      // build final pre-filter and remaining conditions
      (
        composeCondition(rexBuilder, leftLocal.toList),
        composeCondition(rexBuilder, remainingAnds.toList))
    } else {
      val remainingAnds = remainingEquals ++ joinInfo.nonEquiConditions.asScala
      (None, composeCondition(rexBuilder, remainingAnds.toList))
    }
  }

  private def composeCondition(rexBuilder: RexBuilder, rexNodes: List[RexNode]): Option[RexNode] = {
    val condition = RexUtil.composeConjunction(rexBuilder, rexNodes.asJava)
    if (condition.isAlwaysTrue) {
      None
    } else {
      Some(condition)
    }
  }

  /**
   * Analyze potential lookup keys (including [[Constant]] and [[FieldRef]]) of the temporal table
   * from the join condition and calc program on the temporal table.
   *
   * @param rexBuilder
   *   the RexBuilder
   * @param joinKeyPairs
   *   join key pairs from left input field index to temporal table field index
   * @param calcOnTemporalTable
   *   the calc program on temporal table
   * @return
   *   all the potential lookup keys
   */
  private def analyzeLookupKeys(
      rexBuilder: RexBuilder,
      joinKeyPairs: Array[IntPair],
      calcOnTemporalTable: Option[RexProgram]): Map[Int, FunctionParam] = {
    // field_index_in_table_source => constant_lookup_key
    val constantLookupKeys = new mutable.HashMap[Int, Constant]
    // analyze constant lookup keys
    if (calcOnTemporalTable.isDefined && null != calcOnTemporalTable.get.getCondition) {
      val program = calcOnTemporalTable.get
      val condition =
        RexUtil.toCnf(cluster.getRexBuilder, program.expandLocalRef(program.getCondition))
      // presume 'A = 1 AND A = 2' will be reduced to ALWAYS_FALSE
      extractConstantFieldsFromEquiCondition(condition, constantLookupKeys)
    }
    val fieldRefLookupKeys = joinKeyPairs.map(p => (p.target, new FieldRef(p.source)))
    constantLookupKeys.toMap[Int, FunctionParam] ++ fieldRefLookupKeys.toMap[Int, FunctionParam]
  }

  /** Check if lookup key contains primary key, include constant lookup keys. */
  def lookupKeyContainsPrimaryKey(): Boolean = {
    val outputPkIdx = getOutputIndexesOfTemporalTablePrimaryKey
    // use allLookupKeys instead of joinInfo.rightSet because there may exists constant
    // lookup key(s) which are not included in joinInfo.rightKeys.
    outputPkIdx.nonEmpty && outputPkIdx.forall(index => allLookupKeys.contains(index))
  }

  /** Get final output pk indexes if exists, otherwise will get empty. */
  def getOutputIndexesOfTemporalTablePrimaryKey: Array[Int] = {
    val temporalPkIdxs = getPrimaryKeyIndexesOfTemporalTable
    val NO_PK = Array.empty[Int]
    val outputPkIdx = if (temporalPkIdxs.isEmpty) {
      NO_PK
    } else {
      calcOnTemporalTable match {
        case Some(program) =>
          val outputMapping = program.getProjectList.asScala.zipWithIndex
            .map { case (ref, index) => (index, program.expandLocalRef(ref)) }
            .map {
              case (outIndex, ref) =>
                ref match {
                  case inputRef: RexInputRef => (inputRef.getIndex, outIndex)
                  case _ => (-1, -1)
                }
            }
            .toMap
          val outputPk = temporalPkIdxs.forall(outputMapping.contains)
          if (outputPk) {
            // remapping pk index
            temporalPkIdxs.map(outputMapping)
          } else {
            NO_PK
          }

        case None => temporalPkIdxs
      }
    }

    outputPkIdx
  }

  private def getPrimaryKeyIndexesOfTemporalTable: Array[Int] = {
    // get primary key columns of lookup table if exists
    val pkColumns = getPrimaryKeyColumnsOfTemporalTable
    if (pkColumns.isDefined) {
      val newSchema = temporalTable.getRowType.getFieldNames
      pkColumns.get.toArray().map(col => newSchema.indexOf(col))
    } else {
      Array[Int]()
    }
  }

  private def getPrimaryKeyColumnsOfTemporalTable: Option[util.List[String]] = {
    temporalTable match {
      case t: TableSourceTable =>
        convert(t.contextResolvedTable.getResolvedSchema.getPrimaryKey)
      case t: IntermediateRelTable =>
        t.relNode match {
          case scan: TableScan =>
            convert(
              scan.getTable
                .asInstanceOf[TableSourceTable]
                .contextResolvedTable
                .getResolvedSchema
                .getPrimaryKey)
          case _ =>
            throw new TableException(
              "Unexpected exception: the node inside intermediate table must be a table source scan")
        }
      case t: LegacyTableSourceTable[_] =>
        val pkConstraint = t.catalogTable.getSchema.getPrimaryKey
        // the UniqueConstraint in old TableSchema has different package name
        if (pkConstraint.isPresent) {
          Option.apply(pkConstraint.get().getColumns)
        } else {
          Option.empty[util.List[String]]
        }
    }
  }

  private def convert(pkConstraint: Optional[UniqueConstraint]): Option[util.List[String]] = {
    if (pkConstraint.isPresent) {
      Option.apply(pkConstraint.get().getColumns)
    } else {
      Option.empty[util.List[String]]
    }
  }

  // ----------------------------------------------------------------------------------------
  //                             Physical Optimization Utilities
  // ----------------------------------------------------------------------------------------

  private def extractConstantFieldsFromEquiCondition(
      condition: RexNode,
      constantFieldMap: mutable.HashMap[Int, Constant]): Unit = condition match {
    case c: RexCall if c.getKind == SqlKind.AND =>
      c.getOperands.asScala.foreach(r => extractConstantField(r, constantFieldMap))
    case rex: RexNode => extractConstantField(rex, constantFieldMap)
    case _ =>
  }

  private def extractConstantField(
      pred: RexNode,
      constantFieldMap: mutable.HashMap[Int, Constant]): Unit = pred match {
    case c: RexCall if c.getKind == SqlKind.EQUALS =>
      val left = c.getOperands.get(0)
      val right = c.getOperands.get(1)
      val (inputRef, literal) = (left, right) match {
        case (literal: RexLiteral, ref: RexInputRef) => (ref, literal)
        case (ref: RexInputRef, literal: RexLiteral) => (ref, literal)
        case _ => return // non-constant condition
      }
      val dataType = FlinkTypeFactory.toLogicalType(inputRef.getType)
      constantFieldMap.put(inputRef.getIndex, new Constant(dataType, literal))
    case _ => // ignore
  }

  // ----------------------------------------------------------------------------------------
  //                              toString Utilities
  // ----------------------------------------------------------------------------------------

  private def joinConditionToString(
      resultFieldNames: Array[String],
      expressionFormat: ExpressionFormat = ExpressionFormat.Prefix,
      sqlExplainLevel: SqlExplainLevel): String = {

    def appendCondition(sb: StringBuilder, cond: Option[RexNode]): Unit = {
      cond match {
        case Some(condition) =>
          sb.append(
            getExpressionString(
              condition,
              resultFieldNames.toList,
              None,
              expressionFormat,
              sqlExplainLevel))
        case _ =>
      }
    }

    if (finalPreFilterCondition.isEmpty && finalRemainingCondition.isEmpty) {
      "N/A"
    } else {
      val sb = new StringBuilder
      appendCondition(sb, finalPreFilterCondition)
      appendCondition(sb, finalRemainingCondition)
      sb.toString()
    }
  }
}
