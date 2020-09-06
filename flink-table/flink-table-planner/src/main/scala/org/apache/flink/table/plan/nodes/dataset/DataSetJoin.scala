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

package org.apache.flink.table.plan.nodes.dataset

import java.lang.Iterable
import java.lang.{Boolean => JBool}

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.functions.{FilterFunction, FlatJoinFunction, GroupReduceFunction, JoinFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.{TableConfig, TableException, Types}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.runtime._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataSetJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    rowRelDataType: RelDataType,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    joinInfo: JoinInfo,
    keyPairs: List[IntPair],
    joinType: JoinRelType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonJoin
  with DataSetRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    joinToString(
      joinRowType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    joinExplainTerms(
      super.explainTerms(pw),
      joinRowType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val leftRowCnt = metadata.getRowCount(getLeft)
    val leftRowSize = estimateRowSize(getLeft.getRowType)

    val rightRowCnt = metadata.getRowCount(getRight)
    val rightRowSize = estimateRowSize(getRight.getRowType)

    val ioCost = (leftRowCnt * leftRowSize) + (rightRowCnt * rightRowSize)
    val cpuCost = leftRowCnt + rightRowCnt
    val rowCnt = leftRowCnt + rightRowCnt

    planner.getCostFactory.makeCost(rowCnt, cpuCost, ioCost)
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl): DataSet[Row] = {

    val config = tableEnv.getConfig

    val returnType = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      // if no equality keys => not supported
      throw new TableException(
        "Joins should have at least one equality condition.\n" +
          s"\tLeft: ${left.toString},\n" +
          s"\tRight: ${right.toString},\n" +
          s"\tCondition: (${joinConditionToString(joinRowType,
            joinCondition, getExpressionString)})"
      )
    }
    else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach(pair => {
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.add(pair.source)
          rightKeys.add(pair.target)
        } else {
          throw new TableException(
            "Equality join predicate on incompatible types.\n" +
              s"\tLeft: ${left.toString},\n" +
              s"\tRight: ${right.toString},\n" +
              s"\tCondition: (${joinConditionToString(joinRowType,
                joinCondition, getExpressionString)})"
          )
        }
      })
    }

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    joinType match {
      case JoinRelType.INNER =>
        addInnerJoin(
          leftDataSet,
          rightDataSet,
          leftKeys.toArray,
          rightKeys.toArray,
          returnType,
          config)
      case JoinRelType.LEFT =>
        addLeftOuterJoin(
          leftDataSet,
          rightDataSet,
          leftKeys.toArray,
          rightKeys.toArray,
          returnType,
          config)
      case JoinRelType.RIGHT =>
        addRightOuterJoin(
          leftDataSet,
          rightDataSet,
          leftKeys.toArray,
          rightKeys.toArray,
          returnType,
          config)
      case JoinRelType.FULL =>
        addFullOuterJoin(
          leftDataSet,
          rightDataSet,
          leftKeys.toArray,
          rightKeys.toArray,
          returnType,
          config)
      case _ => throw new TableException(s"$joinType is not supported.")
    }
  }

  private def addInnerJoin(
      left: DataSet[Row],
      right: DataSet[Row],
      leftKeys: Array[Int],
      rightKeys: Array[Int],
      resultType: TypeInformation[Row],
      config: TableConfig): DataSet[Row] = {

    val generator = new FunctionCodeGenerator(
      config,
      false,
      left.getType,
      Some(right.getType))
    val conversion = generator.generateConverterResultExpression(
      resultType,
      joinRowType.getFieldNames)

    val condition = generator.generateExpression(joinCondition)
    val body =
      s"""
         |${condition.code}
         |if (${condition.resultTerm}) {
         |  ${conversion.code}
         |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
         |}
         |""".stripMargin

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Row, Row, Row]],
      body,
      resultType)

    val joinFun = new FlatJoinRunner[Row, Row, Row](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)

    left.join(right)
      .where(leftKeys: _*)
      .equalTo(rightKeys: _*)
      .`with`(joinFun)
      .name(getJoinOpName)
  }

  private def addLeftOuterJoin(
      left: DataSet[Row],
      right: DataSet[Row],
      leftKeys: Array[Int],
      rightKeys: Array[Int],
      resultType: TypeInformation[Row],
      config: TableConfig): DataSet[Row] = {

    if (!config.getNullCheck) {
      throw new TableException("Null check in TableConfig must be enabled for outer joins.")
    }

    val joinOpName = getJoinOpName

    // replace field names by indexed names for easier key handling
    val leftType = new RowTypeInfo(left.getType.asInstanceOf[RowTypeInfo].getFieldTypes: _*)
    val rightType = right.getType.asInstanceOf[RowTypeInfo]

    // partition and sort left input
    // this step ensures we can reuse the sorting for all following operations
    // (groupBy->join->groupBy)
    val partitionedSortedLeft: DataSet[Row] = partitionAndSort(left, leftKeys)

    // fold identical rows of the left input
    val foldedRowsLeft: DataSet[Row] = foldIdenticalRows(partitionedSortedLeft, leftType)

    // create JoinFunction to evaluate join predicate
    val predFun = generatePredicateFunction(leftType, rightType, config)
    val joinOutType = new RowTypeInfo(leftType, rightType, Types.INT)
    val joinFun = new LeftOuterJoinRunner(predFun.name, predFun.code, joinOutType)

    // join left and right inputs, evaluate join predicate, and emit join pairs
    val nestedLeftKeys = leftKeys.map(i => s"f0.f$i")
    val joinPairs = foldedRowsLeft.leftOuterJoin(right, JoinHint.REPARTITION_SORT_MERGE)
      .where(nestedLeftKeys: _*)
      .equalTo(rightKeys: _*)
      .`with`(joinFun)
      .withForwardedFieldsFirst("f0->f0")
      .name(joinOpName)

    // create GroupReduceFunction to generate the join result
    val convFun = generateConversionFunction(leftType, rightType, resultType, config)
    val reduceFun = new LeftOuterJoinGroupReduceRunner(
      convFun.name,
      convFun.code,
      convFun.returnType)

    // convert join pairs to result.
    // This step ensures we preserve the rows of the left input.
    joinPairs
      .groupBy("f0")
      .reduceGroup(reduceFun)
      .name(joinOpName)
      .returns(resultType)
  }

  private def addRightOuterJoin(
      left: DataSet[Row],
      right: DataSet[Row],
      leftKeys: Array[Int],
      rightKeys: Array[Int],
      resultType: TypeInformation[Row],
      config: TableConfig): DataSet[Row] = {

    if (!config.getNullCheck) {
      throw new TableException("Null check in TableConfig must be enabled for outer joins.")
    }

    val joinOpName = getJoinOpName

    // replace field names by indexed names for easier key handling
    val leftType = left.getType.asInstanceOf[RowTypeInfo]
    val rightType = new RowTypeInfo(right.getType.asInstanceOf[RowTypeInfo].getFieldTypes: _*)

    // partition and sort right input
    // this step ensures we can reuse the sorting for all following operations
    // (groupBy->join->groupBy)
    val partitionedSortedRight: DataSet[Row] = partitionAndSort(right, rightKeys)

    // fold identical rows of the right input
    val foldedRowsRight: DataSet[Row] = foldIdenticalRows(partitionedSortedRight, rightType)

    // create JoinFunction to evaluate join predicate
    val predFun = generatePredicateFunction(leftType, rightType, config)
    val joinOutType = new RowTypeInfo(leftType, rightType, Types.INT)
    val joinFun = new RightOuterJoinRunner(predFun.name, predFun.code, joinOutType)

    // join left and right inputs, evaluate join predicate, and emit join pairs
    val nestedRightKeys = rightKeys.map(i => s"f0.f$i")
    val joinPairs = left.rightOuterJoin(foldedRowsRight, JoinHint.REPARTITION_SORT_MERGE)
      .where(leftKeys: _*)
      .equalTo(nestedRightKeys: _*)
      .`with`(joinFun)
      .withForwardedFieldsSecond("f0->f1")
      .name(joinOpName)

    // create GroupReduceFunction to generate the join result
    val convFun = generateConversionFunction(leftType, rightType, resultType, config)
    val reduceFun = new RightOuterJoinGroupReduceRunner(
      convFun.name,
      convFun.code,
      convFun.returnType)

    // convert join pairs to result
    // This step ensures we preserve the rows of the right input.
    joinPairs
      .groupBy("f1")
      .reduceGroup(reduceFun)
      .name(joinOpName)
      .returns(resultType)
  }

  private def addFullOuterJoin(
      left: DataSet[Row],
      right: DataSet[Row],
      leftKeys: Array[Int],
      rightKeys: Array[Int],
      resultType: TypeInformation[Row],
      config: TableConfig): DataSet[Row] = {

    if (!config.getNullCheck) {
      throw new TableException("Null check in TableConfig must be enabled for outer joins.")
    }

    val joinOpName = getJoinOpName

    // replace field names by indexed names for easier key handling
    val leftType = new RowTypeInfo(left.getType.asInstanceOf[RowTypeInfo].getFieldTypes: _*)
    val rightType = new RowTypeInfo(right.getType.asInstanceOf[RowTypeInfo].getFieldTypes: _*)

    // partition and sort left and right input
    // this step ensures we can reuse the sorting for all following operations
    // (groupBy->join->groupBy), except the second grouping to preserve right rows.
    val partitionedSortedLeft: DataSet[Row] = partitionAndSort(left, leftKeys)
    val partitionedSortedRight: DataSet[Row] = partitionAndSort(right, rightKeys)

    // fold identical rows of the left and right input
    val foldedRowsLeft: DataSet[Row] = foldIdenticalRows(partitionedSortedLeft, leftType)
    val foldedRowsRight: DataSet[Row] = foldIdenticalRows(partitionedSortedRight, rightType)

    // create JoinFunction to evaluate join predicate
    val predFun = generatePredicateFunction(leftType, rightType, config)
    val joinOutType = new RowTypeInfo(leftType, rightType, Types.INT, Types.INT)
    val joinFun = new FullOuterJoinRunner(predFun.name, predFun.code, joinOutType)

    // join left and right inputs, evaluate join predicate, and emit join pairs
    val nestedLeftKeys = leftKeys.map(i => s"f0.f$i")
    val nestedRightKeys = rightKeys.map(i => s"f0.f$i")
    val joinPairs = foldedRowsLeft
      .fullOuterJoin(foldedRowsRight, JoinHint.REPARTITION_SORT_MERGE)
      .where(nestedLeftKeys: _*)
      .equalTo(nestedRightKeys: _*)
      .`with`(joinFun)
      .withForwardedFieldsFirst("f0->f0")
      .withForwardedFieldsSecond("f0->f1")
      .name(joinOpName)

    // create GroupReduceFunctions to generate the join result
    val convFun = generateConversionFunction(leftType, rightType, resultType, config)
    val leftReduceFun = new LeftFullOuterJoinGroupReduceRunner(
      convFun.name,
      convFun.code,
      convFun.returnType)
    val rightReduceFun = new RightFullOuterJoinGroupReduceRunner(
      convFun.name,
      convFun.code,
      convFun.returnType)

    // compute joined (left + right) and left preserved (left + null)
    val joinedAndLeftPreserved = joinPairs
      // filter for pairs with left row
      .filter(new FilterFunction[Row](){
        override def filter(row: Row): Boolean = row.getField(0) != null})
      .groupBy("f0")
      .reduceGroup(leftReduceFun)
      .name(joinOpName)
      .returns(resultType)

    // compute right preserved (null + right)
    val rightPreserved = joinPairs
      // filter for pairs with right row
      .filter(new FilterFunction[Row](){
        override def filter(row: Row): Boolean = row.getField(1) != null})
      .groupBy("f1")
      .reduceGroup(rightReduceFun)
      .name(joinOpName)
      .returns(resultType)

    // union joined (left + right), left preserved (left + null), and right preserved (null + right)
    joinedAndLeftPreserved.union(rightPreserved)
  }

  private def getJoinOpName: String = {
    s"where: (${joinConditionToString(joinRowType, joinCondition, getExpressionString)}), " +
      s"join: (${joinSelectionToString(joinRowType)})"
  }

  /** Returns an array of indices with some indices being a prefix. */
  private def getFullIndiciesWithPrefix(keys: Array[Int], numFields: Int): Array[Int] = {
    // get indices of all fields which are not keys
    val nonKeys = (0 until numFields).filter(!keys.contains(_))
    // return all field indices prefixed by keys
    keys ++ nonKeys
  }

  /**
    * Partitions the data set on the join keys and sort it on all field with the join keys being a
    * prefix.
    */
  private def partitionAndSort(
      dataSet: DataSet[Row],
      partitionKeys: Array[Int]): DataSet[Row] = {

    // construct full sort keys with partitionKeys being a prefix
    val sortKeys = getFullIndiciesWithPrefix(partitionKeys, dataSet.getType.getArity)
    // partition
    val partitioned: DataSet[Row] = dataSet.partitionByHash(partitionKeys: _*)
    // sort on all fields
    sortKeys.foldLeft(partitioned: DataSet[Row]) { (d, i) =>
      d.sortPartition(i, Order.ASCENDING).asInstanceOf[DataSet[Row]]
    }
  }

  /**
    * Folds identical rows of a data set into a single row with a duplicate count.
    */
  private def foldIdenticalRows(
      dataSet: DataSet[Row],
      dataSetType: TypeInformation[Row]): DataSet[Row] = {

    val resultType = new RowTypeInfo(dataSetType, Types.INT)
    val groupKeys = 0 until dataSetType.getArity

    dataSet
      // group on all fields of the input row
      .groupBy(groupKeys: _*)
      // fold identical rows
      .reduceGroup(new GroupReduceFunction[Row, Row] {
        val outTuple = new Row(2)
        override def reduce(values: Iterable[Row], out: Collector[Row]): Unit = {
          // count number of duplicates
          var cnt = 0
          val it = values.iterator()
          while (it.hasNext) {
            // set output row
            outTuple.setField(0, it.next())
            cnt += 1
          }
          // set count
          outTuple.setField(1, cnt)
          // emit folded row with count
          out.collect(outTuple)
        }
      })
      .returns(resultType)
      .withForwardedFields("*->f0")
      .name("fold identical rows")
  }

  /**
    * Generates a [[GeneratedFunction]] of a [[JoinFunction]] to evaluate the join predicate.
    * The function returns the result of the predicate as [[JBool]].
    */
  private def generatePredicateFunction(
      leftType: TypeInformation[Row],
      rightType: TypeInformation[Row],
      config: TableConfig): GeneratedFunction[JoinFunction[Row, Row, JBool], JBool] = {
    val predGenerator = new FunctionCodeGenerator(config, false, leftType, Some(rightType))
    val condition = predGenerator.generateExpression(joinCondition)
    val predCode =
      s"""
         |${condition.code}
         |return (${condition.resultTerm});
         |""".stripMargin

    predGenerator.generateFunction(
      "OuterJoinPredicate",
      classOf[JoinFunction[Row, Row, JBool]],
      predCode,
      Types.BOOLEAN)
  }

  /**
    * Generates a [[GeneratedFunction]] of a [[JoinFunction]] to produce the join result.
    */
  private def generateConversionFunction(
      leftType: TypeInformation[Row],
      rightType: TypeInformation[Row],
      resultType: TypeInformation[Row],
      config: TableConfig): GeneratedFunction[JoinFunction[Row, Row, Row], Row] = {

    val conversionGenerator = new FunctionCodeGenerator(config, true, leftType, Some(rightType))
    val conversion = conversionGenerator.generateConverterResultExpression(
      resultType,
      joinRowType.getFieldNames)
    val convCode =
      s"""
         |${conversion.code}
         |return ${conversion.resultTerm};
         |""".stripMargin

    conversionGenerator.generateFunction(
      "OuterJoinConverter",
      classOf[JoinFunction[Row, Row, Row]],
      convCode,
      resultType)
  }

}
