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
package org.apache.flink.table.plan.nodes.common

import com.google.common.primitives.Primitives
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TypeExtractor}
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.operators.ProcessOperator
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.`type`._
import org.apache.flink.table.api.{TableConfig, TableException, TableSchema}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.LookupJoinCodeGenerator._
import org.apache.flink.table.codegen.{CodeGeneratorContext, LookupJoinCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getParamClassesConsiderVarArgs, getUserDefinedMethod, signatureToString, signaturesToString}
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.plan.util.{JoinTypeUtil, RelExplainUtil}
import org.apache.flink.table.plan.util.LookupJoinUtil._
import org.apache.flink.table.runtime.join.lookup.{AsyncLookupJoinRunner, LookupJoinRunner, AsyncLookupJoinWithCalcRunner, LookupJoinWithCalcRunner}
import org.apache.flink.table.sources.TableIndex.IndexType
import org.apache.flink.table.sources.{LookupConfig, LookupableTableSource, TableIndex, TableSource}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.types.Row

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Common abstract RelNode for temporal table join which shares most methods.
  * @param input  input rel node
  * @param tableSource  the table source to be temporal joined
  * @param tableRowType  the row type of the table source
  * @param calcOnTemporalTable  the calc (projection&filter) after table scan before joining
  */
abstract class CommonLookupJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val tableSource: TableSource[_],
    tableRowType: RelDataType,
    val calcOnTemporalTable: Option[RexProgram],
    val joinInfo: JoinInfo,
    val joinType: JoinRelType)
  extends SingleRel(cluster, traitSet, input)
  with FlinkRelNode {

  val joinKeyPairs: Array[IntPair] = getTemporalTableJoinKeyPairs(joinInfo, calcOnTemporalTable)
  val indexKeys: Array[TableIndex] = getTableIndexes(tableSource)
  // all potential index keys, mapping from field index in table source to LookupKey
  val allLookupKeys: Map[Int, LookupKey] = analyzeLookupKeys(
    cluster.getRexBuilder,
    joinKeyPairs,
    indexKeys,
    tableSource.getTableSchema,
    calcOnTemporalTable)
  // the matched best lookup fields which is in defined order, maybe empty
  val matchedLookupFields: Option[Array[Int]] = findMatchedIndex(
    indexKeys,
    tableSource.getTableSchema,
    allLookupKeys)

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val rightType = if (calcOnTemporalTable.isDefined) {
      calcOnTemporalTable.get.getOutputRowType
    } else {
      tableRowType
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
    val remaining = joinInfo.getRemaining(cluster.getRexBuilder)
    val joinCondition = if (remaining.isAlwaysTrue) {
      None
    } else {
      Some(remaining)
    }

    val inputFieldNames = input.getRowType.getFieldNames.asScala.toArray
    val tableFieldNames = tableSource.getTableSchema.getFieldNames
    val rightFieldNames = calcOnTemporalTable match {
      case Some(calc) => calc.getOutputRowType.getFieldNames.asScala.toArray
      case None => tableFieldNames
    }
    val resultFieldNames = getRowType.getFieldNames.asScala.toArray
    val lookupConfig = getLookupConfig(tableSource.asInstanceOf[LookupableTableSource[_]])
    val whereString = calcOnTemporalTable match {
      case Some(calc) => RelExplainUtil.conditionToString(calc, getExpressionString)
      case None => "N/A"
    }

    super.explainTerms(pw)
      .item("table", tableSource.explainSource())
      .item("joinType", JoinTypeUtil.getFlinkJoinType(joinType))
      .item("async", lookupConfig.isAsyncEnabled)
      .item("on", joinOnToString(inputFieldNames, rightFieldNames, joinInfo))
      .itemIf("where", whereString, calcOnTemporalTable.isDefined)
      .itemIf("joinCondition",
        joinConditionToString(resultFieldNames, joinCondition),
        joinCondition.isDefined)
      .item("select", joinSelectionToString(getRowType))
  }

  // ----------------------------------------------------------------------------------------
  //                             Physical Translation
  // ----------------------------------------------------------------------------------------

  def translateToPlanInternal(
      inputTransformation: StreamTransformation[BaseRow],
      env: StreamExecutionEnvironment,
      config: TableConfig,
      relBuilder: RelBuilder): StreamTransformation[BaseRow] = {

    val inputRowType = FlinkTypeFactory.toInternalRowType(input.getRowType)
    val tableSourceRowType = FlinkTypeFactory.toInternalRowType(tableRowType)
    val resultRowType = FlinkTypeFactory.toInternalRowType(getRowType)
    val tableSchema = tableSource.getTableSchema

    // validate whether the node is valid and supported.
    validate(
      tableSource,
      inputRowType,
      tableSourceRowType,
      indexKeys,
      allLookupKeys,
      matchedLookupFields,
      joinType)

    val lookupFieldsInOrder = matchedLookupFields.get
    val lookupFieldNamesInOrder = lookupFieldsInOrder.map(tableSchema.getFieldNames()(_))
    val lookupFieldTypesInOrder = lookupFieldsInOrder
      .map(tableSchema.getFieldTypes()(_))
      .map(TypeConverters.createInternalTypeFromTypeInfo)
    val remainingCondition = getRemainingJoinCondition(
      cluster.getRexBuilder,
      relBuilder,
      input.getRowType,
      tableRowType,
      calcOnTemporalTable,
      lookupFieldsInOrder,
      joinKeyPairs,
      joinInfo,
      allLookupKeys)

    val lookupableTableSource = tableSource.asInstanceOf[LookupableTableSource[_]]
    val lookupConfig = getLookupConfig(lookupableTableSource)
    val leftOuterJoin = joinType == JoinRelType.LEFT

    val operator = if (lookupConfig.isAsyncEnabled) {
      val asyncBufferCapacity= lookupConfig.getAsyncBufferCapacity
      val asyncTimeout = lookupConfig.getAsyncTimeoutMs

      val asyncLookupFunction = lookupableTableSource
        .getAsyncLookupFunction(lookupFieldNamesInOrder)
      // return type valid check
      val udtfResultType = asyncLookupFunction.getResultType
      val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
        asyncLookupFunction,
        classOf[AsyncTableFunction[_]],
        asyncLookupFunction.getClass,
        0)
      checkUdtfReturnType(
        tableSource.explainSource(),
        tableSource.getReturnType,
        udtfResultType,
        extractedResultTypeInfo)
      val parameters = Array(new GenericType(classOf[ResultFuture[_]])) ++ lookupFieldTypesInOrder
      checkEvalMethodSignature(
        asyncLookupFunction,
        parameters,
        extractedResultTypeInfo)

      val generatedFetcher = LookupJoinCodeGenerator.generateAsyncLookupFunction(
        config,
        relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory],
        inputRowType,
        resultRowType,
        tableSource.getReturnType,
        lookupFieldsInOrder,
        allLookupKeys,
        asyncLookupFunction)

      val asyncFunc = if (calcOnTemporalTable.isDefined) {
        // a projection or filter after table source scan
        val rightRowType = FlinkTypeFactory
          .toInternalRowType(calcOnTemporalTable.get.getOutputRowType)
        val generatedResultFuture = LookupJoinCodeGenerator.generateTableAsyncCollector(
          config,
          "TableFunctionResultFuture",
          inputRowType,
          rightRowType,
          remainingCondition)
        val generatedCalc = generateCalcMapFunction(
          config,
          calcOnTemporalTable,
          tableSourceRowType)

        new AsyncLookupJoinWithCalcRunner(
          generatedFetcher,
          generatedCalc,
          generatedResultFuture,
          tableSource.getReturnType,
          rightRowType.toTypeInfo,
          leftOuterJoin,
          lookupConfig.getAsyncBufferCapacity)
      } else {
        // right type is the same as table source row type, because no calc after temporal table
        val rightRowType = tableSourceRowType
        val generatedResultFuture = LookupJoinCodeGenerator.generateTableAsyncCollector(
          config,
          "TableFunctionResultFuture",
          inputRowType,
          rightRowType,
          remainingCondition)
        new AsyncLookupJoinRunner(
          generatedFetcher,
          generatedResultFuture,
          tableSource.getReturnType,
          rightRowType.toTypeInfo,
          leftOuterJoin,
          asyncBufferCapacity)
      }

      // force ORDERED output mode currently, optimize it to UNORDERED
      // when the downstream do not need orderness
      new AsyncWaitOperator(asyncFunc, asyncTimeout, asyncBufferCapacity, OutputMode.ORDERED)
    } else {
      // sync join
      val lookupFunction = lookupableTableSource.getLookupFunction(lookupFieldNamesInOrder)
      // return type valid check
      val udtfResultType = lookupFunction.getResultType
      val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
        lookupFunction,
        classOf[TableFunction[_]],
        lookupFunction.getClass,
        0)
      checkUdtfReturnType(
        tableSource.explainSource(),
        tableSource.getReturnType,
        udtfResultType,
        extractedResultTypeInfo)
      checkEvalMethodSignature(
        lookupFunction,
        lookupFieldTypesInOrder,
        extractedResultTypeInfo)

      val generatedFetcher = LookupJoinCodeGenerator.generateLookupFunction(
        config,
        relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory],
        inputRowType,
        resultRowType,
        tableSource.getReturnType,
        lookupFieldsInOrder,
        allLookupKeys,
        lookupFunction,
        env.getConfig.isObjectReuseEnabled)

      val ctx = CodeGeneratorContext(config)
      val processFunc = if (calcOnTemporalTable.isDefined) {
        // a projection or filter after table source scan
        val rightRowType = FlinkTypeFactory
          .toInternalRowType(calcOnTemporalTable.get.getOutputRowType)
        val generatedCollector = generateCollector(
          ctx,
          inputRowType,
          rightRowType,
          resultRowType,
          remainingCondition,
          None)
        val generatedCalc = generateCalcMapFunction(
          config,
          calcOnTemporalTable,
          tableSourceRowType)

        new LookupJoinWithCalcRunner(
          generatedFetcher,
          generatedCalc,
          generatedCollector,
          leftOuterJoin,
          rightRowType.getArity)
      } else {
        // right type is the same as table source row type, because no calc after temporal table
        val rightRowType = tableSourceRowType
        val generatedCollector = generateCollector(
          ctx,
          inputRowType,
          rightRowType,
          resultRowType,
          remainingCondition,
          None)
        new LookupJoinRunner(
          generatedFetcher,
          generatedCollector,
          leftOuterJoin,
          rightRowType.getArity)
      }
      new ProcessOperator(processFunc)
    }

    new OneInputTransformation(
      inputTransformation,
      "LookupJoin",
      operator,
      resultRowType.toTypeInfo,
      inputTransformation.getParallelism)
  }

  def getLookupConfig(tableSource: LookupableTableSource[_]): LookupConfig = {
    if (tableSource.getLookupConfig != null) {
      tableSource.getLookupConfig
    } else {
      LookupConfig.DEFAULT
    }
  }

  private def rowTypeEquals(expected: TypeInformation[_], actual: TypeInformation[_]): Boolean = {
    // check internal and external type, cause we will auto convert external class to internal
    // class (eg: Row => BaseRow).
    (expected.getTypeClass == classOf[BaseRow] || expected.getTypeClass == classOf[Row]) &&
      (actual.getTypeClass == classOf[BaseRow] || actual.getTypeClass == classOf[Row])
  }

  def checkEvalMethodSignature(
      func: UserDefinedFunction,
      expectedTypes: Array[InternalType],
      udtfReturnType: TypeInformation[_])
    : Array[Class[_]] = {
    val expectedTypeClasses = if (udtfReturnType.getTypeClass == classOf[Row]) {
      expectedTypes.map(InternalTypeUtils.getExternalClassForType)
    } else {
      expectedTypes.map{
        case gt: GenericType[_] => gt.getTypeInfo.getTypeClass  // special case for generic type
        case t@_ => InternalTypeUtils.getInternalClassForType(t)
      }
    }
    val method = getUserDefinedMethod(
      func,
      "eval",
      expectedTypeClasses,
      expectedTypes,
      _ => expectedTypes.indices.map(_ => null).toArray,
      parameterTypeEquals,
      (_, _) => false).getOrElse {
      val msg = s"Given parameter types of the lookup TableFunction of TableSource " +
        s"[${tableSource.explainSource()}] do not match the expected signature.\n" +
        s"Expected: eval${signatureToString(expectedTypeClasses)} \n" +
        s"Actual: eval${signaturesToString(func, "eval")}"
      throw new TableException(msg)
    }
    getParamClassesConsiderVarArgs(method.isVarArgs,
      method.getParameterTypes, expectedTypes.length)
  }

  private def parameterTypeEquals(candidate: Class[_], expected: Class[_]): Boolean = {
    candidate == null ||
      candidate == expected ||
      expected == classOf[Object] ||
      candidate == classOf[Object] || // Special case when we don't know the type
      expected.isPrimitive && Primitives.wrap(expected) == candidate ||
      (candidate.isArray &&
        expected.isArray &&
        candidate.getComponentType.isInstanceOf[Object] &&
        expected.getComponentType == classOf[Object])
  }

  private def getRemainingJoinCondition(
      rexBuilder: RexBuilder,
      relBuilder: RelBuilder,
      leftRelDataType: RelDataType,
      tableRelDataType: RelDataType,
      calcOnTemporalTable: Option[RexProgram],
      checkedLookupFields: Array[Int],
      joinKeyPairs: Array[IntPair],
      joinInfo: JoinInfo,
      allLookupKeys: Map[Int, LookupKey]): Option[RexNode] = {
    val remainingPairs = joinKeyPairs.filter(p => !checkedLookupFields.contains(p.target))
    // convert remaining pairs to RexInputRef tuple for building sqlStdOperatorTable.EQUALS calls
    val remainingAnds = remainingPairs.map { p =>
      val leftFieldType = leftRelDataType.getFieldList.get(p.source).getType
      val leftInputRef = new RexInputRef(p.source, leftFieldType)
      val rightInputRef = calcOnTemporalTable match {
        case Some(program) =>
          val rightKeyIdx = program
            .getOutputRowType.getFieldNames
            .indexOf(program.getInputRowType.getFieldNames.get(p.target))
          new RexInputRef(
            leftRelDataType.getFieldCount + rightKeyIdx,
            program.getOutputRowType.getFieldList.get(rightKeyIdx).getType)

        case None =>
          new RexInputRef(
            leftRelDataType.getFieldCount + p.target,
            tableRelDataType.getFieldList.get(p.target).getType)
      }
      (leftInputRef, rightInputRef)
    }
    val equiAnds = relBuilder.and(remainingAnds.map(p => relBuilder.equals(p._1, p._2)): _*)
    val condition = relBuilder.and(equiAnds, joinInfo.getRemaining(rexBuilder))
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
  def analyzeLookupKeys(
      rexBuilder: RexBuilder,
      joinKeyPairs: Array[IntPair],
      tableIndexes: Array[TableIndex],
      temporalTableSchema: TableSchema,
      calcOnTemporalTable: Option[RexProgram]): Map[Int, LookupKey] = {
    val fieldNames = temporalTableSchema.getFieldNames
    val allIndexFields = tableIndexes
      .flatMap(_.getIndexedColumns.asScala.map(fieldNames.indexOf(_)))
      .toSet
    // field_index_in_table_source => constant_lookup_key
    val constantLookupKeys = new mutable.HashMap[Int, ConstantLookupKey]
    // analyze constant lookup keys
    if (calcOnTemporalTable.isDefined && null != calcOnTemporalTable.get.getCondition) {
      val program = calcOnTemporalTable.get
      val condition = RexUtil.toCnf(
        cluster.getRexBuilder,
        program.expandLocalRef(program.getCondition))
      // presume 'A = 1 AND A = 2' will be reduced to ALWAYS_FALSE
      extractConstantFieldsFromEquiCondition(condition, allIndexFields, constantLookupKeys)
    }
    val fieldRefLookupKeys = joinKeyPairs.map(p => (p.target, FieldRefLookupKey(p.source)))
    (constantLookupKeys ++ fieldRefLookupKeys).toMap
  }

  private def findMatchedIndex(
      tableIndexes: Array[TableIndex],
      temporalTableSchema: TableSchema,
      allLookupKeys: Map[Int, LookupKey]): Option[Array[Int]] = {

    val fieldNames = temporalTableSchema.getFieldNames

    // [(indexFields, isUniqueIndex)]
    val indexes: Array[(Array[Int], Boolean)] = tableIndexes.map { tableIndex =>
      val indexFields = tableIndex.getIndexedColumns.asScala.map(fieldNames.indexOf(_)).toArray
      val isUniqueIndex = tableIndex.getIndexType.equals(IndexType.UNIQUE)
      (indexFields, isUniqueIndex)
    }

    val matchedIndexes = indexes.filter(_._1.forall(allLookupKeys.contains))
    if (matchedIndexes.length > 1) {
      // find a best one, we prefer a unique index key here
      val uniqueIndex = matchedIndexes.find(_._2).map(_._1)
      if (uniqueIndex.isDefined) {
        uniqueIndex
      } else {
        // all the matched index are normal index, select anyone from matched indexes
        matchedIndexes.map(_._1).headOption
      }
    } else {
      // select anyone from matched indexes
      matchedIndexes.map(_._1).headOption
    }
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
      allIndexFields: Set[Int],
      constantFieldMap: mutable.HashMap[Int, ConstantLookupKey]): Unit = condition match {
    case c: RexCall if c.getKind == SqlKind.AND =>
      c.getOperands.asScala.foreach(r => extractConstantField(r, allIndexFields, constantFieldMap))
    case rex: RexNode => extractConstantField(rex, allIndexFields, constantFieldMap)
    case _ =>
  }

  private def extractConstantField(
      pred: RexNode,
      allIndexFields: Set[Int],
      constantFieldMap: mutable.HashMap[Int, ConstantLookupKey]): Unit = pred match {
    case c: RexCall if c.getKind == SqlKind.EQUALS =>
      val left = c.getOperands.get(0)
      val right = c.getOperands.get(1)
      val (inputRef, literal) = (left, right) match {
        case (literal: RexLiteral, ref: RexInputRef) => (ref, literal)
        case (ref: RexInputRef, literal: RexLiteral) => (ref, literal)
      }
      if (allIndexFields.contains(inputRef.getIndex)) {
        val dataType = FlinkTypeFactory.toInternalType(inputRef.getType)
        constantFieldMap.put(inputRef.getIndex, ConstantLookupKey(dataType, literal))
      }
    case _ => // ignore
  }

  // ----------------------------------------------------------------------------------------
  //                                       Validation
  // ----------------------------------------------------------------------------------------

  def validate(
      tableSource: TableSource[_],
      inputRowType: RowType,
      tableSourceRowType: RowType,
      tableIndexes: Array[TableIndex],
      allLookupKeys: Map[Int, LookupKey],
      matchedLookupFields: Option[Array[Int]],
      joinType: JoinRelType): Unit = {

    // checked PRIMARY KEY or (UNIQUE) INDEX is defined.
    if (tableIndexes.isEmpty) {
      throw new TableException(
        s"Temporal table join requires table [${tableSource.explainSource()}] defines " +
          s"a PRIMARY KEY or (UNIQUE) INDEX.")
    }

    // check join on all fields of PRIMARY KEY or (UNIQUE) INDEX
    if (allLookupKeys.isEmpty || matchedLookupFields.isEmpty) {
      throw new TableException(
        "Temporal table join requires an equality condition on ALL fields of " +
          s"table [${tableSource.explainSource()}]'s PRIMARY KEY or (UNIQUE) INDEX(s).")
    }

    if (!tableSource.isInstanceOf[LookupableTableSource[_]]) {
      throw new TableException(s"TableSource of [${tableSource.explainSource()}] must " +
        s"implement LookupableTableSource interface if it is used in temporal table join.")
    }

    val checkedLookupFields = matchedLookupFields.get

    val lookupKeyPairs = joinKeyPairs.filter(p => checkedLookupFields.contains(p.target))
    val leftKeys = lookupKeyPairs.map(_.source)
    val rightKeys = lookupKeyPairs.map(_.target)
    val leftKeyTypes = leftKeys.map(inputRowType.getFieldTypes()(_))
    // use original keyPair to validate key types (rigthKeys may include constant keys)
    val rightKeyTypes = rightKeys.map(tableSourceRowType.getFieldTypes()(_))

    // check type
    val incompatibleConditions = new mutable.ArrayBuffer[String]()
    for (i <- lookupKeyPairs.indices) {
      val leftType = leftKeyTypes(i)
      val rightType = rightKeyTypes(i)
      if (leftType != rightType) {
        val leftName = inputRowType.getFieldNames()(i)
        val rightName = tableSourceRowType.getFieldNames()(i)
        val condition = s"$leftName[$leftType]=$rightName[$rightType]"
        incompatibleConditions += condition
      }
    }
    if (incompatibleConditions.nonEmpty) {
      throw new TableException(s"Temporal table join requires equivalent condition " +
        s"of the same type, but the condition is ${incompatibleConditions.mkString(", ")}")
    }

    if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
      throw new TableException(
        "Temporal table join currently only support INNER JOIN and LEFT JOIN, " +
          "but was " + joinType.toString + " JOIN")
    }

    val tableReturnType = tableSource.getReturnType
    if (!tableReturnType.isInstanceOf[BaseRowTypeInfo] &&
      !tableReturnType.isInstanceOf[RowTypeInfo]) {
      throw new TableException(
        "Temporal table join only support Row or BaseRow type as return type of temporal table." +
          " But was " + tableReturnType)
    }

    // success
  }

  def checkUdtfReturnType(
      tableDesc: String,
      tableReturnTypeInfo: TypeInformation[_],
      udtfReturnTypeInfo: TypeInformation[_],
      extractedUdtfReturnTypeInfo: TypeInformation[_]): Unit = {
    if (udtfReturnTypeInfo == null) {
      if (!rowTypeEquals(tableReturnTypeInfo, extractedUdtfReturnTypeInfo)) {
        throw new TableException(
          s"The TableSource [$tableDesc] return type $tableReturnTypeInfo does not match " +
            s"its lookup function extracted return type $extractedUdtfReturnTypeInfo")
      }
      if (extractedUdtfReturnTypeInfo.getTypeClass != classOf[BaseRow] &&
        extractedUdtfReturnTypeInfo.getTypeClass != classOf[Row]) {
        throw new TableException(
          s"Result type of the lookup TableFunction of TableSource [$tableDesc] is " +
            s"$extractedUdtfReturnTypeInfo type, " +
            s"but currently only Row and BaseRow are supported.")
      }
    } else {
      if (!rowTypeEquals(tableReturnTypeInfo, udtfReturnTypeInfo)) {
        throw new TableException(
          s"The TableSource [$tableDesc] return type $tableReturnTypeInfo " +
            s"does not match its lookup function return type $udtfReturnTypeInfo")
      }
      if (!udtfReturnTypeInfo.isInstanceOf[BaseRowTypeInfo] &&
        !udtfReturnTypeInfo.isInstanceOf[RowTypeInfo]) {
        throw new TableException(
          "Result type of the async lookup TableFunction of TableSource " +
            s"'$tableDesc' is $udtfReturnTypeInfo type, " +
            s"currently only Row and BaseRow are supported.")
      }
    }
  }

  // ----------------------------------------------------------------------------------------
  //                              toString Utilities
  // ----------------------------------------------------------------------------------------

  private def joinSelectionToString(resultType: RelDataType): String = {
    resultType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString(
      resultFieldNames: Array[String],
      joinCondition: Option[RexNode]): String = joinCondition match {
    case Some(condition) =>
      getExpressionString(condition, resultFieldNames.toList, None)
    case None => "N/A"
  }

  private def joinOnToString(
      inputFieldNames: Array[String],
      tableFieldNames: Array[String],
      joinInfo: JoinInfo): String = {
    val keyPairNames = joinInfo.pairs().asScala.map { p =>
      s"${inputFieldNames(p.source)}=${
        if (p.target >= 0 && p.target < tableFieldNames.length) tableFieldNames(p.target) else -1
      }"
    }
    keyPairNames.mkString(", ")
  }
}

