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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.planner.plan.nodes.{ExpressionFormat, FlinkRelNode}
import org.apache.flink.table.planner.plan.schema.{LegacyTableSourceTable, TableSourceTable}
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil._
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.plan.utils.{JoinTypeUtil, LookupJoinUtil, RelExplainUtil}

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.mapping.IntPair

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Common abstract RelNode for temporal table join which shares most methods.
  *
  * For a lookup join query:
  *
  * <pre>
  * SELECT T.id, T.content, D.age
  * FROM T JOIN userTable FOR SYSTEM_TIME AS OF T.proctime AS D
  * ON T.content = concat(D.name, '!') AND D.age = 11 AND T.id = D.id
  * WHERE D.name LIKE 'Jack%'
  * </pre>
  *
  * The LookupJoin physical node encapsulates the following RelNode tree:
  *
  * <pre>
  *      Join (l.name = r.name)
  *    /     \
  * RelNode  Calc (concat(name, "!") as name, name LIKE 'Jack%')
  *           |
  *        DimTable (lookup-keys: age=11, id=l.id)
  *     (age, id, name)
  * </pre>
  *
  * The important member fields in LookupJoin:
  * <ul>
  *  <li>allLookupKeys: [$0=11, $1=l.id] ($0 and $1 is the indexes of age and id in dim table)</li>
  *  <li>remainingCondition: l.name=r.name</li>
  * <ul>
  *
  * The workflow of lookup join:
  *
  * 1) lookup records dimension table using the lookup-keys <br>
  * 2) project & filter on the lookup-ed records <br>
  * 3) join left input record and lookup-ed records <br>
  * 4) only outputs the rows which match to the remainingCondition <br>
  *
  * @param input  input rel node
  * @param calcOnTemporalTable  the calc (projection&filter) after table scan before joining
  */
abstract class CommonPhysicalLookupJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    // TODO: refactor this into TableSourceTable, once legacy TableSource is removed
    temporalTable: RelOptTable,
    val calcOnTemporalTable: Option[RexProgram],
    val joinInfo: JoinInfo,
    val joinType: JoinRelType)
  extends SingleRel(cluster, traitSet, input)
  with FlinkRelNode {

  val allLookupKeys: Map[Int, LookupKey] = {
    // join key pairs from left input field index to temporal table field index
    val joinKeyPairs: Array[IntPair] = getTemporalTableJoinKeyPairs(joinInfo, calcOnTemporalTable)
    // all potential index keys, mapping from field index in table source to LookupKey
    analyzeLookupKeys(
      cluster.getRexBuilder,
      joinKeyPairs,
      calcOnTemporalTable)
  }
  // remaining condition the filter joined records (left input record X lookup-ed records)
  val remainingCondition: Option[RexNode] = getRemainingJoinCondition(
    cluster.getRexBuilder,
    input.getRowType,
    calcOnTemporalTable,
    allLookupKeys.keys.toList.sorted.toArray,
    joinInfo)

  if (containsPythonCall(joinInfo.getRemaining(cluster.getRexBuilder))) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val rightType = if (calcOnTemporalTable.isDefined) {
      calcOnTemporalTable.get.getOutputRowType
    } else {
      temporalTable.getRowType
    }
    SqlValidatorUtil.deriveJoinRowType(
      input.getRowType,
      rightType,
      joinType,
      flinkTypeFactory,
      null,
      Collections.emptyList[RelDataTypeField])
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputFieldNames = input.getRowType.getFieldNames.asScala.toArray
    val tableFieldNames = temporalTable.getRowType.getFieldNames
    val resultFieldNames = getRowType.getFieldNames.asScala.toArray
    val whereString = calcOnTemporalTable match {
      case Some(calc) =>
        RelExplainUtil.conditionToString(calc, getExpressionString, preferExpressionFormat(pw))
      case None => ""
    }
    val lookupKeys = allLookupKeys.map {
      case (tableField, fieldKey: FieldRefLookupKey) =>
        s"${tableFieldNames.get(tableField)}=${inputFieldNames(fieldKey.index)}"
      case (tableField, constantKey: ConstantLookupKey) =>
        s"${tableFieldNames.get(tableField)}=${RelExplainUtil.literalToString(constantKey.literal)}"
    }.mkString(", ")
    val selection = calcOnTemporalTable match {
      case Some(calc) =>
        val rightSelect = RelExplainUtil.selectionToString(
          calc,
          getExpressionString,
          preferExpressionFormat(pw))
        inputFieldNames.mkString(", ") + ", " + rightSelect
      case None =>
        resultFieldNames.mkString(", ")
    }
    val tableIdentifier: ObjectIdentifier = temporalTable match {
      case t: TableSourceTable => t.tableIdentifier
      case t: LegacyTableSourceTable[_] => t.tableIdentifier
    }

    val lookupFunction: UserDefinedFunction =
      LookupJoinUtil.getLookupFunction(temporalTable, allLookupKeys.keys.map(Int.box).toList.asJava)
    val isAsyncEnabled: Boolean = lookupFunction match {
      case _: TableFunction[_] => false
      case _: AsyncTableFunction[_] => true
    }


    super.explainTerms(pw)
      .item("table", tableIdentifier.asSummaryString())
      .item("joinType", JoinTypeUtil.getFlinkJoinType(joinType))
      .item("async", isAsyncEnabled)
      .item("lookup", lookupKeys)
      .itemIf("where", whereString, whereString.nonEmpty)
      .itemIf("joinCondition",
        joinConditionToString(resultFieldNames, remainingCondition, preferExpressionFormat(pw)),
        remainingCondition.isDefined)
      .item("select", selection)
  }

  /**
    * Gets the remaining join condition which is used
    */
  private def getRemainingJoinCondition(
      rexBuilder: RexBuilder,
      leftRelDataType: RelDataType,
      calcOnTemporalTable: Option[RexProgram],
      checkedLookupFields: Array[Int],
      joinInfo: JoinInfo): Option[RexNode] = {

    // indexes of right key field
    val rightKeyIndexes = calcOnTemporalTable match {
      case Some(program) =>
        checkedLookupFields.map { lookupFieldIndex => // lookupFieldIndex is field index on table
          program
            .getOutputRowType.getFieldNames
            .indexOf(program.getInputRowType.getFieldNames.get(lookupFieldIndex))
        }
      case None =>
        checkedLookupFields
    }
    val joinPairs = joinInfo.pairs().asScala.toArray
    val remainingPairs = joinPairs.filter(p => !rightKeyIndexes.contains(p.target))
    val joinRowType = getRowType
    // convert remaining pairs to RexInputRef tuple for building SqlStdOperatorTable.EQUALS calls
    val remainingEquals = remainingPairs.map { p =>
      val leftFieldType = leftRelDataType.getFieldList.get(p.source).getType
      val leftInputRef = new RexInputRef(p.source, leftFieldType)
      val rightIndex = leftRelDataType.getFieldCount + p.target
      val rightFieldType = joinRowType.getFieldList.get(rightIndex).getType
      val rightInputRef = new RexInputRef(rightIndex, rightFieldType)
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftInputRef, rightInputRef)
    }
    val remainingAnds = remainingEquals ++ joinInfo.nonEquiConditions.asScala
    // build a new condition
    val condition = RexUtil.composeConjunction(
      rexBuilder,
      remainingAnds.toList.asJava)
    if (condition.isAlwaysTrue) {
      None
    } else {
      Some(condition)
    }
  }


  /**
    * Gets the join key pairs from left input field index to temporal table field index
    * @param joinInfo the join information of temporal table join
    * @param calcOnTemporalTable the calc programs on temporal table
    */
  private def getTemporalTableJoinKeyPairs(
      joinInfo: JoinInfo,
      calcOnTemporalTable: Option[RexProgram]): Array[IntPair] = {
    val joinPairs = joinInfo.pairs().asScala.toArray
    calcOnTemporalTable match {
      case Some(program) =>
        // the target key of joinInfo is the calc output fields, we have to remapping to table here
        val keyPairs = new mutable.ArrayBuffer[IntPair]()
        joinPairs.map {
          p =>
            val calcSrcIdx = getIdenticalSourceField(program, p.target)
            if (calcSrcIdx != -1) {
              keyPairs += new IntPair(p.source, calcSrcIdx)
            }
        }
        keyPairs.toArray
      case None => joinPairs
    }
  }

  /**
    * Analyze potential lookup keys (including [[ConstantLookupKey]] and [[FieldRefLookupKey]])
    * of the temporal table from the join condition and calc program on the temporal table.
    *
    * @param rexBuilder the RexBuilder
    * @param joinKeyPairs join key pairs from left input field index to temporal table field index
    * @param calcOnTemporalTable  the calc program on temporal table
    * @return all the potential lookup keys
    */
  private def analyzeLookupKeys(
      rexBuilder: RexBuilder,
      joinKeyPairs: Array[IntPair],
      calcOnTemporalTable: Option[RexProgram]): Map[Int, LookupKey] = {
    // field_index_in_table_source => constant_lookup_key
    val constantLookupKeys = new mutable.HashMap[Int, ConstantLookupKey]
    // analyze constant lookup keys
    if (calcOnTemporalTable.isDefined && null != calcOnTemporalTable.get.getCondition) {
      val program = calcOnTemporalTable.get
      val condition = RexUtil.toCnf(
        cluster.getRexBuilder,
        program.expandLocalRef(program.getCondition))
      // presume 'A = 1 AND A = 2' will be reduced to ALWAYS_FALSE
      extractConstantFieldsFromEquiCondition(condition, constantLookupKeys)
    }
    val fieldRefLookupKeys = joinKeyPairs.map(p => (p.target, new FieldRefLookupKey(p.source)))
    constantLookupKeys.toMap[Int, LookupKey] ++ fieldRefLookupKeys.toMap[Int, LookupKey]
  }

  // ----------------------------------------------------------------------------------------
  //                             Physical Optimization Utilities
  // ----------------------------------------------------------------------------------------

  // this is highly inspired by Calcite's RexProgram#getSourceField(int)
  private def getIdenticalSourceField(rexProgram: RexProgram, outputOrdinal: Int): Int = {
    assert((outputOrdinal >= 0) && (outputOrdinal < rexProgram.getProjectList.size()))
    val project = rexProgram.getProjectList.get(outputOrdinal)
    var index = project.getIndex
    while (true) {
      var expr = rexProgram.getExprList.get(index)
      expr match {
        case call: RexCall if call.getOperator == SqlStdOperatorTable.IN_FENNEL =>
          // drill through identity function
          expr = call.getOperands.get(0)
        case call: RexCall if call.getOperator == SqlStdOperatorTable.CAST =>
          // drill through identity function
          expr = call.getOperands.get(0)
        case _ =>
      }
      expr match {
        case ref: RexLocalRef => index = ref.getIndex
        case ref: RexInputRef => return ref.getIndex
        case _ => return -1
      }
    }
    -1
  }

  private def extractConstantFieldsFromEquiCondition(
      condition: RexNode,
      constantFieldMap: mutable.HashMap[Int, ConstantLookupKey]): Unit = condition match {
    case c: RexCall if c.getKind == SqlKind.AND =>
      c.getOperands.asScala.foreach(r => extractConstantField(r, constantFieldMap))
    case rex: RexNode => extractConstantField(rex, constantFieldMap)
    case _ =>
  }

  private def extractConstantField(
      pred: RexNode,
      constantFieldMap: mutable.HashMap[Int, ConstantLookupKey]): Unit = pred match {
    case c: RexCall if c.getKind == SqlKind.EQUALS =>
      val left = c.getOperands.get(0)
      val right = c.getOperands.get(1)
      val (inputRef, literal) = (left, right) match {
        case (literal: RexLiteral, ref: RexInputRef) => (ref, literal)
        case (ref: RexInputRef, literal: RexLiteral) => (ref, literal)
        case _ => return // non-constant condition
      }
      val dataType = FlinkTypeFactory.toLogicalType(inputRef.getType)
      constantFieldMap.put(inputRef.getIndex, new ConstantLookupKey(dataType, literal))
    case _ => // ignore
  }

  // ----------------------------------------------------------------------------------------
  //                              toString Utilities
  // ----------------------------------------------------------------------------------------

  private def joinConditionToString(
      resultFieldNames: Array[String],
      joinCondition: Option[RexNode],
      expressionFormat: ExpressionFormat = ExpressionFormat.Prefix): String = joinCondition match {
    case Some(condition) =>
      getExpressionString(condition, resultFieldNames.toList, None, expressionFormat)
    case None => "N/A"
  }
}
