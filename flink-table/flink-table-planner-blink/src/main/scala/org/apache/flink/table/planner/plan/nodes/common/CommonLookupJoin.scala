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
package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory
import org.apache.flink.streaming.api.operators.{ProcessOperator, SimpleOperatorFactory, StreamOperatorFactory}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.table.connector.source.{AsyncTableFunctionProvider, LookupTableSource, TableFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.conversion.{DataStructureConverter, DataStructureConverters}
import org.apache.flink.table.functions.UserDefinedFunctionHelper.prepareInstance
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator._
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, LookupJoinCodeGenerator}
import org.apache.flink.table.planner.plan.nodes.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.planner.plan.nodes.{ExpressionFormat, FlinkRelNode}
import org.apache.flink.table.planner.plan.schema.{LegacyTableSourceTable, TableSourceTable}
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil._
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.plan.utils.{JoinTypeUtil, RelExplainUtil}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext
import org.apache.flink.table.runtime.operators.join.lookup.{AsyncLookupJoinRunner, AsyncLookupJoinWithCalcRunner, LookupJoinRunner, LookupJoinWithCalcRunner}
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isInteroperable
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.runtime.typeutils.{InternalSerializers, InternalTypeInfo}
import org.apache.flink.table.sources.LookupableTableSource
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.tools.RelBuilder
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
  *  <li>joinPairs: "0=0" (equal condition of Join)</li>
  *  <li>joinKeyPairs: empty (left input field index to dim table field index)</li>
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
abstract class CommonLookupJoin(
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

  // join key pairs from left input field index to temporal table field index
  val joinKeyPairs: Array[IntPair] = getTemporalTableJoinKeyPairs(joinInfo, calcOnTemporalTable)
  // all potential index keys, mapping from field index in table source to LookupKey
  val allLookupKeys: Map[Int, LookupKey] = analyzeLookupKeys(
    cluster.getRexBuilder,
    joinKeyPairs,
    calcOnTemporalTable)
  val lookupKeyIndicesInOrder: Array[Int] = allLookupKeys.keys.toList.sorted.toArray
  // remaining condition the filter joined records (left input record X lookup-ed records)
  val remainingCondition: Option[RexNode] = getRemainingJoinCondition(
    cluster.getRexBuilder,
    input.getRowType,
    calcOnTemporalTable,
    allLookupKeys.keys.toList.sorted.toArray,
    joinKeyPairs,
    joinInfo,
    allLookupKeys)

  // ----------------------------------------------------------------------------------------
  // Member fields initialized based on TableSource type
  // ----------------------------------------------------------------------------------------

  lazy val lookupFunction: UserDefinedFunction = {
    temporalTable match {
      case t: TableSourceTable =>
        // TODO: support nested lookup keys in the future,
        //  currently we only support top-level lookup keys
        val indices = lookupKeyIndicesInOrder.map(Array(_))
        val tableSource = t.tableSource.asInstanceOf[LookupTableSource]
        val providerContext = new LookupRuntimeProviderContext(indices)
        val provider = tableSource.getLookupRuntimeProvider(providerContext)
        provider match {
          case tf: TableFunctionProvider[_] => tf.createTableFunction()
          case atf: AsyncTableFunctionProvider[_] => atf.createAsyncTableFunction()
        }
      case t: LegacyTableSourceTable[_] =>
        val lookupFieldNamesInOrder = lookupKeyIndicesInOrder
          .map(temporalTable.getRowType.getFieldNames.get(_))
        val tableSource = t.tableSource.asInstanceOf[LookupableTableSource[_]]
        if (tableSource.isAsyncEnabled) {
          tableSource.getAsyncLookupFunction(lookupFieldNamesInOrder)
        } else {
          tableSource.getLookupFunction(lookupFieldNamesInOrder)
        }
    }
  }

  lazy val isAsyncEnabled: Boolean = lookupFunction match {
    case _: TableFunction[_] => false
    case _: AsyncTableFunction[_] => true
  }

  lazy val tableSourceDescription: String = temporalTable match {
    case t: TableSourceTable =>
      s"DynamicTableSource [${t.tableSource.asSummaryString()}]"
    case t: LegacyTableSourceTable[_] =>
      s"TableSource [${t.tableSource.explainSource()}]"
  }

  lazy val tableIdentifier: ObjectIdentifier = temporalTable match {
    case t: TableSourceTable => t.tableIdentifier
    case t: LegacyTableSourceTable[_] => t.tableIdentifier
  }

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

  // ----------------------------------------------------------------------------------------
  //                             Physical Translation
  // ----------------------------------------------------------------------------------------

  def translateToPlanInternal(
      inputTransformation: Transformation[RowData],
      env: StreamExecutionEnvironment,
      config: TableConfig,
      relBuilder: RelBuilder): Transformation[RowData] = {

    val inputRowType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
    val tableSourceRowType = FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType)
    val resultRowType = FlinkTypeFactory.toLogicalRowType(getRowType)

    // validate whether the node is valid and supported.
    validate(
      config.getConfiguration,
      inputRowType,
      tableSourceRowType,
      allLookupKeys,
      joinType)

    val isLeftOuterJoin = joinType == JoinRelType.LEFT

    val operatorFactory = if (isAsyncEnabled) {
      createAsyncLookupJoin(
        config,
        relBuilder,
        inputRowType,
        tableSourceRowType,
        resultRowType,
        isLeftOuterJoin)
    } else {
       createSyncLookupJoin(
        config,
        relBuilder,
        inputRowType,
        tableSourceRowType,
        resultRowType,
        isLeftOuterJoin,
        env.getConfig.isObjectReuseEnabled)
    }

    ExecNodeUtil.createOneInputTransformation(
      inputTransformation,
      getRelDetailedDescription,
      operatorFactory,
      InternalTypeInfo.of(resultRowType),
      inputTransformation.getParallelism,
      0)
  }

  private def createAsyncLookupJoin(
      config: TableConfig,
      relBuilder: RelBuilder,
      inputRowType: RowType,
      tableSourceRowType: RowType,
      resultRowType: RowType,
      isLeftOuterJoin: Boolean)
    : StreamOperatorFactory[RowData] = {

    val asyncBufferCapacity= config.getConfiguration
        .getInteger(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY)
    val asyncTimeout = config.getConfiguration.get(
      ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT).toMillis

    val asyncLookupFunction = lookupFunction.asInstanceOf[AsyncTableFunction[_]]
    val dataTypeFactory = unwrapContext(relBuilder).getCatalogManager.getDataTypeFactory

    val (generatedFetcher, fetcherDataType) = LookupJoinCodeGenerator.generateAsyncLookupFunction(
      config,
      dataTypeFactory,
      inputRowType,
      tableSourceRowType,
      resultRowType,
      allLookupKeys,
      lookupKeyIndicesInOrder,
      asyncLookupFunction,
      temporalTable.getQualifiedName.asScala.mkString("."))

    val fetcherConverter = DataStructureConverters.getConverter(fetcherDataType)
        .asInstanceOf[DataStructureConverter[RowData, AnyRef]]

    val asyncFunc = if (calcOnTemporalTable.isDefined) {
      // a projection or filter after table source scan
      val rightRowType = FlinkTypeFactory
        .toLogicalRowType(calcOnTemporalTable.get.getOutputRowType)
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
        fetcherConverter,
        generatedCalc,
        generatedResultFuture,
        InternalSerializers.create(rightRowType),
        isLeftOuterJoin,
        asyncBufferCapacity)
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
        fetcherConverter,
        generatedResultFuture,
        InternalSerializers.create(rightRowType),
        isLeftOuterJoin,
        asyncBufferCapacity)
    }

    // force ORDERED output mode currently, optimize it to UNORDERED
    // when the downstream do not need orderness
    new AsyncWaitOperatorFactory(asyncFunc, asyncTimeout, asyncBufferCapacity, OutputMode.ORDERED)
  }

  private def createSyncLookupJoin(
      config: TableConfig,
      relBuilder: RelBuilder,
      inputRowType: RowType,
      tableSourceRowType: RowType,
      resultRowType: RowType,
      isLeftOuterJoin: Boolean,
      isObjectReuseEnabled: Boolean)
    : StreamOperatorFactory[RowData] = {

    val syncLookupFunction = lookupFunction.asInstanceOf[TableFunction[_]]
    val dataTypeFactory = unwrapContext(relBuilder).getCatalogManager.getDataTypeFactory

    val generatedFetcher = LookupJoinCodeGenerator.generateSyncLookupFunction(
      config,
      dataTypeFactory,
      inputRowType,
      tableSourceRowType,
      resultRowType,
      allLookupKeys,
      lookupKeyIndicesInOrder,
      syncLookupFunction,
      temporalTable.getQualifiedName.asScala.mkString("."),
      isObjectReuseEnabled)

    val ctx = CodeGeneratorContext(config)
    val processFunc = if (calcOnTemporalTable.isDefined) {
      // a projection or filter after table source scan
      val rightRowType = FlinkTypeFactory
        .toLogicalRowType(calcOnTemporalTable.get.getOutputRowType)
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
        isLeftOuterJoin,
        rightRowType.getFieldCount)
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
        isLeftOuterJoin,
        rightRowType.getFieldCount)
    }
    SimpleOperatorFactory.of(new ProcessOperator(processFunc))
  }

  /**
    * Gets the remaining join condition which is used
    */
  private def getRemainingJoinCondition(
      rexBuilder: RexBuilder,
      leftRelDataType: RelDataType,
      calcOnTemporalTable: Option[RexProgram],
      checkedLookupFields: Array[Int],
      joinKeyPairs: Array[IntPair],
      joinInfo: JoinInfo,
      allLookupKeys: Map[Int, LookupKey]): Option[RexNode] = {

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
  //                                       Validation
  // ----------------------------------------------------------------------------------------

  private def validate(
      config: ReadableConfig,
      inputRowType: RowType,
      tableSourceRowType: RowType,
      allLookupKeys: Map[Int, LookupKey],
      joinType: JoinRelType)
    : Unit = {

    // validate table source and function implementation first
    validateTableSource()
    prepareInstance(config, lookupFunction)

    // check join on all fields of PRIMARY KEY or (UNIQUE) INDEX
    if (allLookupKeys.isEmpty) {
      throw new TableException(
        "Temporal table join requires an equality condition on fields of " +
          s"table [${tableIdentifier.asSummaryString()}].")
    }

    val lookupKeyPairs = joinKeyPairs.filter(p => allLookupKeys.contains(p.target))
    val leftKeys = lookupKeyPairs.map(_.source)
    val rightKeys = lookupKeyPairs.map(_.target)
    val leftKeyTypes = leftKeys.map(inputRowType.getTypeAt)
    // use original keyPair to validate key types (rigthKeys may include constant keys)
    val rightKeyTypes = rightKeys.map(tableSourceRowType.getTypeAt)

    // check type
    val incompatibleConditions = new mutable.ArrayBuffer[String]()
    for (i <- lookupKeyPairs.indices) {
      val leftType = leftKeyTypes(i)
      val rightType = rightKeyTypes(i)
      if (!isInteroperable(leftType, rightType)) {
        val leftName = inputRowType.getFieldNames.get(i)
        val rightName = tableSourceRowType.getFieldNames.get(i)
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
    // success
  }

  private def validateTableSource(): Unit = temporalTable match {
    case t: TableSourceTable =>
      if (!t.tableSource.isInstanceOf[LookupTableSource]) {
        throw new TableException(s"$tableSourceDescription must " +
          s"implement LookupTableSource interface if it is used in temporal table join.")
      }
    case t: LegacyTableSourceTable[_] =>
      val tableSource = t.tableSource
      if (!tableSource.isInstanceOf[LookupableTableSource[_]]) {
        throw new TableException(s"$tableSourceDescription must " +
          s"implement LookupableTableSource interface if it is used in temporal table join.")
      }
      val tableSourceProducedType = fromDataTypeToTypeInfo(tableSource.getProducedDataType)
      if (!tableSourceProducedType.isInstanceOf[InternalTypeInfo[RowData]] &&
        !tableSourceProducedType.isInstanceOf[RowTypeInfo]) {
        throw new TableException(
          "Temporal table join only support Row or RowData type as return type of temporal table." +
            " But was " + tableSourceProducedType)
      }
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
