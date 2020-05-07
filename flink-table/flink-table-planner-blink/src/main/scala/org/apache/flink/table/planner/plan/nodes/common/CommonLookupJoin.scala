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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo, TypeExtractor}
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory
import org.apache.flink.streaming.api.operators.{ProcessOperator, SimpleOperatorFactory}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{TableConfig, TableException, TableSchema}
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.table.connector.source.{AsyncTableFunctionProvider, LookupTableSource, TableFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator._
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, LookupJoinCodeGenerator}
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.{getParamClassesConsiderVarArgs, getUserDefinedMethod, signatureToString, signaturesToString}
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.schema.{LegacyTableSourceTable, TableSourceTable}
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil._
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.plan.utils.{JoinTypeUtil, RelExplainUtil}
import org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext
import org.apache.flink.table.runtime.operators.join.lookup.{AsyncLookupJoinRunner, AsyncLookupJoinWithCalcRunner, LookupJoinRunner, LookupJoinWithCalcRunner}
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.{fromDataTypeToLogicalType, fromLogicalTypeToDataType}
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isInteroperable
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.sources.LookupableTableSource
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass
import org.apache.flink.table.types.logical.{LogicalType, RowType, TypeInformationRawType}
import org.apache.flink.types.Row

import com.google.common.primitives.Primitives
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
import java.util.concurrent.CompletableFuture

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Common abstract RelNode for temporal table join which shares most methods.
  *
  * For a look join query:
  *
  * <pre>
  * SELECT T.id, T.content, D.age
  * FROM T JOIN userTable FOR SYSTEM_TIME AS OF T.proctime AS D
  * ON T.content = concat(D.name, '!') AND D.age = 11 AND T.id = D.id
  * WHERE D.name LIKE 'Jack%'
  * </pre>
  *
  * The LookJoin physical node encapsulates the following RelNode tree:
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

  val temporalTableSchema: TableSchema = FlinkTypeFactory.toTableSchema(temporalTable.getRowType)
  // join key pairs from left input field index to temporal table field index
  val joinKeyPairs: Array[IntPair] = getTemporalTableJoinKeyPairs(joinInfo, calcOnTemporalTable)
  // all potential index keys, mapping from field index in table source to LookupKey
  val allLookupKeys: Map[Int, LookupKey] = analyzeLookupKeys(
    cluster.getRexBuilder,
    joinKeyPairs,
    temporalTableSchema,
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
          .map(temporalTableSchema.getFieldNames()(_))
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
    val tableFieldNames = temporalTableSchema.getFieldNames
    val resultFieldNames = getRowType.getFieldNames.asScala.toArray
    val whereString = calcOnTemporalTable match {
      case Some(calc) =>
        RelExplainUtil.conditionToString(calc, getExpressionString, preferExpressionFormat(pw))
      case None => ""
    }
    val lookupKeys = allLookupKeys.map {
      case (tableField, FieldRefLookupKey(inputField)) =>
        s"${tableFieldNames(tableField)}=${inputFieldNames(inputField)}"
      case (tableField, ConstantLookupKey(_, literal)) =>
        s"${tableFieldNames(tableField)}=${RelExplainUtil.literalToString(literal)}"
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
        joinConditionToString(resultFieldNames, remainingCondition),
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

    val producedTypeInfo = fromDataTypeToTypeInfo(getLookupFunctionProducedType)

    // validate whether the node is valid and supported.
    validate(
      inputRowType,
      tableSourceRowType,
      allLookupKeys,
      joinType)

    val lookupFieldTypesInOrder = lookupKeyIndicesInOrder
      .map(temporalTableSchema.getFieldDataTypes()(_)).map(fromDataTypeToLogicalType)

    val leftOuterJoin = joinType == JoinRelType.LEFT

    val operatorFactory = if (isAsyncEnabled) {
      val asyncBufferCapacity= config.getConfiguration
        .getInteger(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY)
      val asyncTimeout = getMillisecondFromConfigDuration(config,
        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT)

      val asyncLookupFunction = lookupFunction.asInstanceOf[AsyncTableFunction[_]]
      // return type valid check
      val udtfResultType = asyncLookupFunction.getResultType
      val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
        asyncLookupFunction,
        classOf[AsyncTableFunction[_]],
        asyncLookupFunction.getClass,
        0)
      checkUdtfReturnType(
        udtfResultType,
        extractedResultTypeInfo)
      val futureType = new TypeInformationRawType(
        new GenericTypeInfo(classOf[CompletableFuture[_]]))
      val parameters = Array(futureType) ++ lookupFieldTypesInOrder
      checkEvalMethodSignature(
        asyncLookupFunction,
        parameters,
        extractedResultTypeInfo)

      val generatedFetcher = LookupJoinCodeGenerator.generateAsyncLookupFunction(
        config,
        relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory],
        inputRowType,
        resultRowType,
        producedTypeInfo,
        lookupKeyIndicesInOrder,
        allLookupKeys,
        asyncLookupFunction)

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
          generatedCalc,
          generatedResultFuture,
          producedTypeInfo,
          RowDataTypeInfo.of(rightRowType),
          leftOuterJoin,
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
          generatedResultFuture,
          producedTypeInfo,
          RowDataTypeInfo.of(rightRowType),
          leftOuterJoin,
          asyncBufferCapacity)
      }

      // force ORDERED output mode currently, optimize it to UNORDERED
      // when the downstream do not need orderness
      new AsyncWaitOperatorFactory(asyncFunc, asyncTimeout, asyncBufferCapacity, OutputMode.ORDERED)
    } else {
      // sync join
      val syncLookupFunction = lookupFunction.asInstanceOf[TableFunction[_]]
      // return type valid check
      val udtfResultType = syncLookupFunction.getResultType
      val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
        syncLookupFunction,
        classOf[TableFunction[_]],
        syncLookupFunction.getClass,
        0)
      checkUdtfReturnType(
        udtfResultType,
        extractedResultTypeInfo)
      checkEvalMethodSignature(
        syncLookupFunction,
        lookupFieldTypesInOrder,
        extractedResultTypeInfo)

      val generatedFetcher = LookupJoinCodeGenerator.generateLookupFunction(
        config,
        relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory],
        inputRowType,
        resultRowType,
        producedTypeInfo,
        lookupKeyIndicesInOrder,
        allLookupKeys,
        syncLookupFunction,
        env.getConfig.isObjectReuseEnabled)

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
          leftOuterJoin,
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
          leftOuterJoin,
          rightRowType.getFieldCount)
      }
      SimpleOperatorFactory.of(new ProcessOperator(processFunc))
    }

    ExecNode.createOneInputTransformation(
      inputTransformation,
      getRelDetailedDescription,
      operatorFactory,
      RowDataTypeInfo.of(resultRowType),
      inputTransformation.getParallelism)
  }

  private def rowTypeEquals(expected: TypeInformation[_], actual: TypeInformation[_]): Boolean = {
    // check internal and external type, cause we will auto convert external class to internal
    // class (eg: Row => RowData).
    (expected.getTypeClass == classOf[RowData] || expected.getTypeClass == classOf[Row]) &&
      (actual.getTypeClass == classOf[RowData] || actual.getTypeClass == classOf[Row])
  }

  private def checkEvalMethodSignature(
      func: UserDefinedFunction,
      expectedTypes: Array[LogicalType],
      udtfReturnType: TypeInformation[_])
    : Array[Class[_]] = {
    val expectedTypeClasses = if (udtfReturnType.getTypeClass == classOf[Row]) {
      expectedTypes.map(ClassLogicalTypeConverter.getDefaultExternalClassForType)
    } else {
      expectedTypes.map {
        // special case for generic type
        case gt: TypeInformationRawType[_] => gt.getTypeInformation.getTypeClass
        case t@_ => toInternalConversionClass(t)
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
      val msg = s"Given parameter types of the lookup TableFunction of $tableSourceDescription " +
        s"do not match the expected signature.\n" +
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
      temporalTableSchema: TableSchema,
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
    val fieldRefLookupKeys = joinKeyPairs.map(p => (p.target, FieldRefLookupKey(p.source)))
    (constantLookupKeys ++ fieldRefLookupKeys).toMap
  }

  private def getLookupFunctionProducedType: DataType = temporalTable match {
    case t: LegacyTableSourceTable[_] =>
      t.tableSource.getProducedDataType

    case _: TableSourceTable =>
      val rowType = FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType)
      val dataRowType = fromLogicalTypeToDataType(rowType)
      val isRow = lookupFunction match {
        case tf: TableFunction[_] =>
          val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
            tf,
            classOf[TableFunction[_]],
            tf.getClass,
            0)
          extractedResultTypeInfo.getTypeClass == classOf[Row]
        case atf: AsyncTableFunction[_] =>
          val extractedResultTypeInfo = TypeExtractor.createTypeInfo(
            atf,
            classOf[AsyncTableFunction[_]],
            atf.getClass,
            0)
          extractedResultTypeInfo.getTypeClass == classOf[Row]
      }
      if (isRow) {
        // we limit to use default conversion class if using Row
        dataRowType
      } else {
        // bridge to RowData if is not external Row
        dataRowType.bridgedTo(classOf[RowData])
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
      }
      val dataType = FlinkTypeFactory.toLogicalType(inputRef.getType)
      constantFieldMap.put(inputRef.getIndex, ConstantLookupKey(dataType, literal))
    case _ => // ignore
  }

  // ----------------------------------------------------------------------------------------
  //                                       Validation
  // ----------------------------------------------------------------------------------------

  private def validate(
      inputRowType: RowType,
      tableSourceRowType: RowType,
      allLookupKeys: Map[Int, LookupKey],
      joinType: JoinRelType): Unit = {

    // validate table source implementation first
    validateTableSource()

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
      if (!tableSourceProducedType.isInstanceOf[RowDataTypeInfo] &&
        !tableSourceProducedType.isInstanceOf[RowTypeInfo]) {
        throw new TableException(
          "Temporal table join only support Row or RowData type as return type of temporal table." +
            " But was " + tableSourceProducedType)
      }
  }

  private def checkUdtfReturnType(
      udtfReturnTypeInfo: TypeInformation[_],
      extractedUdtfReturnTypeInfo: TypeInformation[_]): Unit = {
    if (udtfReturnTypeInfo != null) {
      if (!udtfReturnTypeInfo.isInstanceOf[RowDataTypeInfo] &&
        !udtfReturnTypeInfo.isInstanceOf[RowTypeInfo]) {
        throw new TableException(
          s"Result type of the async lookup TableFunction of $tableSourceDescription " +
            s"is $udtfReturnTypeInfo type, currently only Row and RowData are supported.")
      }
    } else {
      if (extractedUdtfReturnTypeInfo.getTypeClass != classOf[RowData] &&
        extractedUdtfReturnTypeInfo.getTypeClass != classOf[Row]) {
        throw new TableException(
          s"Result type of the lookup TableFunction of $tableSourceDescription is " +
            s"$extractedUdtfReturnTypeInfo type, " +
            s"but currently only Row and RowData are supported.")
      }
    }
    temporalTable match {
      case t: LegacyTableSourceTable[_] =>
        // Legacy TableSource should check the consistency between UDTF return type
        // and source produced type
        val tableSource = t.tableSource
        val tableSourceProducedType = fromDataTypeToTypeInfo(tableSource.getProducedDataType)
        if (udtfReturnTypeInfo != null) {
          if (!rowTypeEquals(tableSourceProducedType, udtfReturnTypeInfo)) {
            throw new TableException(
              s"The $tableSourceDescription return type $tableSourceProducedType " +
                s"does not match its lookup function return type $udtfReturnTypeInfo")
          }
        } else {
          if (!rowTypeEquals(tableSourceProducedType, extractedUdtfReturnTypeInfo)) {
            throw new TableException(
              s"The $tableSourceDescription return type $tableSourceProducedType does not match " +
                s"its lookup function extracted return type $extractedUdtfReturnTypeInfo")
          }
        }
      case _ =>
        // pass, DynamicTableSource doesn't has produced type
    }
  }

  // ----------------------------------------------------------------------------------------
  //                              toString Utilities
  // ----------------------------------------------------------------------------------------

  private def joinConditionToString(
      resultFieldNames: Array[String],
      joinCondition: Option[RexNode]): String = joinCondition match {
    case Some(condition) =>
      getExpressionString(condition, resultFieldNames.toList, None)
    case None => "N/A"
  }
}
