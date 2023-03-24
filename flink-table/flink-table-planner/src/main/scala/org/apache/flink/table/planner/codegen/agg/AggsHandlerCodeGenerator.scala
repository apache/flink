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
package org.apache.flink.table.planner.codegen.agg

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.table.api.{DataTypes, TableException}
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{DeclarativeAggregateFunction, ImperativeAggregateFunction}
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.toRexInputRef
import org.apache.flink.table.planner.plan.utils.AggregateInfoList
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.dataview.{DataViewSpec, ListViewSpec, MapViewSpec, StateListView, StateMapView}
import org.apache.flink.table.runtime.generated._
import org.apache.flink.table.runtime.groupwindow._
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{BooleanType, IntType, LogicalType, RowType}
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils
import org.apache.flink.util.Collector

import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder

import java.time.ZoneId
import java.util.Optional

/**
 * A code generator for generating [[AggsHandleFunction]].
 *
 * @param copyInputField
 *   copy input field element if true (only mutable type will be copied), set to true if field will
 *   be buffered (such as local aggregate)
 */
class AggsHandlerCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    inputFieldTypes: Seq[LogicalType],
    copyInputField: Boolean) {

  private val inputType = RowType.of(inputFieldTypes: _*)

  /** constant expressions that act like a second input in the parameter indices. */
  private var constants: Seq[RexLiteral] = Seq()
  private var constantExprs: Seq[GeneratedExpression] = Seq()

  /** window properties like window_start and window_end, only used in window aggregates */
  private var namespaceClassName: String = _
  private var windowProperties: Seq[WindowProperty] = Seq()
  private var hasNamespace: Boolean = false
  private var sliceAssignerTerm: String = _
  private var shiftTimeZone: ZoneId = _

  /** Aggregates informations */
  private var accTypeInfo: RowType = _
  private var aggBufferSize: Int = _

  private var mergedAccExternalTypes: Array[DataType] = _
  private var mergedAccOffset: Int = 0
  private var mergedAccOnHeap: Boolean = false

  private var ignoreAggValues: Array[Int] = Array()

  private var isAccumulateNeeded = false
  private var isRetractNeeded = false
  private var isMergeNeeded = false
  private var isWindowSizeNeeded = false

  var valueType: RowType = _

  /**
   * The [[aggBufferCodeGens]] and [[aggActionCodeGens]] will be both created when code generate an
   * [[AggsHandleFunction]] or [[NamespaceAggsHandleFunction]]. They both contain all the same
   * AggCodeGens, but are different in the organizational form. The [[aggBufferCodeGens]] flatten
   * all the AggCodeGens in a flat format. The [[aggActionCodeGens]] organize all the AggCodeGens in
   * a tree format. If there is no distinct aggregate, the [[aggBufferCodeGens]] and
   * [[aggActionCodeGens]] are totally the same.
   *
   * When different aggregate distinct on the same field but on different filter conditions, they
   * will share the same distinct state, see DistinctAggCodeGen.DistinctValueGenerator for more
   * information.
   */

  /**
   * The aggBufferCodeGens is organized according to the agg buffer order, which is in a flat
   * format, and is only used to generate the methods relative to accumulators, Such as
   * [[genCreateAccumulators()]], [[genGetAccumulators()]], [[genSetAccumulators()]].
   *
   * For example if we have : count(*), count(distinct a), count(distinct a) filter d > 5, sum(a),
   * sum(distinct a)
   *
   * then the members of aggBufferCodeGens are organized looks like this:
   * \+----------+-----------+-----------+---------+---------+----------------+ \| count(*) |
   * count(a') | count(a') | sum(a) | sum(a') | distinct(a) a' |
   * \+----------+-----------+-----------+---------+---------+----------------+
   */
  private var aggBufferCodeGens: Array[AggCodeGen] = _

  /**
   * The aggActionCodeGens is organized according to the aggregate calling order, which is in a tree
   * format. Such as the aggregates distinct on the same fields should be accumulated together when
   * distinct is satisfied. And this is only used to generate the methods relative to aggregate
   * action. Such as [[genAccumulate()]], [[genRetract()]], [[genMerge()]].
   *
   * For example if we have : count(*), count(distinct a), count(distinct a) filter d > 5, sum(a),
   * sum(distinct a)
   *
   * then the members of aggActionCodeGens are organized looks like this:
   *
   * | count(*) | sum(a) | distinct(a) a' |                             |
   * |:---------|:-------|:---------------|:----------------------------|
   * |          |        |                | -- count(a')                |
   * |          |        |                | -- count(a') (filter d > 5) |
   * |          |        |                | -- sum(a')                  |
   */
  private var aggActionCodeGens: Array[AggCodeGen] = _

  /** Adds constant expressions that act like a second input in the parameter indices. */
  def withConstants(literals: Seq[RexLiteral]): AggsHandlerCodeGenerator = {
    // create constants
    this.constants = literals
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
    val exprs = literals.map(exprGenerator.generateExpression)
    this.constantExprs = exprs.map(ctx.addReusableConstant)
    this
  }

  /**
   * Tells the generator to generate `accumulate(..)` method for the [[AggsHandleFunction]] and
   * [[NamespaceAggsHandleFunction]]. Default not generate `accumulate(..)` method.
   */
  def needAccumulate(): AggsHandlerCodeGenerator = {
    this.isAccumulateNeeded = true
    this
  }

  /**
   * Tells the generator to generate `retract(..)` method for the [[AggsHandleFunction]] and
   * [[NamespaceAggsHandleFunction]]. Default not generate `retract(..)` method.
   *
   * @return
   */
  def needRetract(): AggsHandlerCodeGenerator = {
    this.isRetractNeeded = true
    this
  }

  /**
   * Tells the generator to generate `merge(..)` method with the merged accumulator information for
   * the [[AggsHandleFunction]] and [[NamespaceAggsHandleFunction]]. Default not generate
   * `merge(..)` method.
   *
   * @param mergedAccOffset
   *   the mergedAcc may come from local aggregate, this is the first buffer offset in the row
   * @param mergedAccOnHeap
   *   true if the mergedAcc is on heap, otherwise
   * @param mergedAccExternalTypes
   *   the merged acc types
   */
  def needMerge(
      mergedAccOffset: Int,
      mergedAccOnHeap: Boolean,
      mergedAccExternalTypes: Array[DataType] = null): AggsHandlerCodeGenerator = {
    this.mergedAccOffset = mergedAccOffset
    this.mergedAccOnHeap = mergedAccOnHeap
    this.mergedAccExternalTypes = mergedAccExternalTypes
    this.isMergeNeeded = true
    this
  }

  def needWindowSize(): AggsHandlerCodeGenerator = {
    this.isWindowSizeNeeded = true
    this
  }

  /** Adds window properties such as window_start, window_end */
  private def initialWindowProperties(
      windowProperties: Seq[WindowProperty],
      windowClass: Class[_],
      shiftTimeZone: ZoneId): Unit = {
    this.windowProperties = windowProperties
    this.namespaceClassName = windowClass.getCanonicalName
    this.hasNamespace = true
    this.shiftTimeZone = shiftTimeZone
  }

  /** Adds aggregate infos into context */
  private def initialAggregateInformation(aggInfoList: AggregateInfoList): Unit = {

    this.accTypeInfo = RowType.of(aggInfoList.getAccTypes.map(fromDataTypeToLogicalType): _*)
    this.aggBufferSize = accTypeInfo.getFieldCount
    var aggBufferOffset: Int = 0

    if (mergedAccExternalTypes == null) {
      mergedAccExternalTypes = aggInfoList.getAccTypes
    }

    val aggCodeGens = aggInfoList.aggInfos.map {
      aggInfo =>
        val filterExpr =
          createFilterExpression(aggInfo.agg.filterArg, aggInfo.aggIndex, aggInfo.agg.name)

        val codegen = aggInfo.function match {
          case _: DeclarativeAggregateFunction =>
            new DeclarativeAggCodeGen(
              ctx,
              aggInfo,
              filterExpr,
              mergedAccOffset,
              aggBufferOffset,
              aggBufferSize,
              inputFieldTypes,
              constants,
              relBuilder)
          case _: ImperativeAggregateFunction[_, _] =>
            new ImperativeAggCodeGen(
              ctx,
              aggInfo,
              filterExpr,
              mergedAccOffset,
              aggBufferOffset,
              aggBufferSize,
              inputFieldTypes,
              constantExprs,
              relBuilder,
              hasNamespace,
              mergedAccOnHeap,
              mergedAccExternalTypes(aggBufferOffset),
              copyInputField)
        }
        aggBufferOffset = aggBufferOffset + aggInfo.externalAccTypes.length
        codegen
    }

    val distinctCodeGens = aggInfoList.distinctInfos.zipWithIndex.map {
      case (distinctInfo, index) =>
        val innerCodeGens = distinctInfo.aggIndexes.map(aggCodeGens(_)).toArray
        val distinctIndex = aggCodeGens.length + index
        val filterExpr = distinctInfo.filterArgs.map(
          createFilterExpression(_, distinctIndex, "distinct aggregate"))
        val codegen = new DistinctAggCodeGen(
          ctx,
          distinctInfo,
          index,
          innerCodeGens,
          filterExpr.toArray,
          constantExprs,
          mergedAccOffset,
          aggBufferOffset,
          aggBufferSize,
          hasNamespace,
          isMergeNeeded,
          mergedAccOnHeap,
          distinctInfo.consumeRetraction,
          copyInputField,
          relBuilder)
        // distinct agg buffer occupies only one field
        aggBufferOffset += 1
        codegen
    }

    val distinctAggIndexes = aggInfoList.distinctInfos.flatMap(_.aggIndexes)
    val nonDistinctAggIndexes = aggCodeGens.indices.filter(!distinctAggIndexes.contains(_)).toArray

    this.aggBufferCodeGens = aggCodeGens ++ distinctCodeGens
    this.aggActionCodeGens = nonDistinctAggIndexes.map(aggCodeGens(_)) ++ distinctCodeGens

    // when input contains retractions, we inserted a count1 agg in the agg list
    // the count1 agg value shouldn't be in the aggregate result
    if (aggInfoList.indexOfCountStar.nonEmpty && aggInfoList.countStarInserted) {
      ignoreAggValues ++= Array(aggInfoList.indexOfCountStar.get)
    }

    // the distinct value shouldn't be in the aggregate result
    if (aggInfoList.distinctInfos.nonEmpty) {
      ignoreAggValues ++= distinctCodeGens.indices.map(_ + aggCodeGens.length)
    }
  }

  /** Creates filter argument access expression, none if no filter */
  private def createFilterExpression(
      filterArg: Int,
      aggIndex: Int,
      aggName: String): Option[Expression] = {

    if (filterArg > 0) {
      val filterType = inputFieldTypes(filterArg)
      if (!filterType.isInstanceOf[BooleanType]) {
        throw new TableException(
          s"filter arg must be boolean, but is $filterType, " +
            s"the aggregate is $aggName.")
      }
      Some(toRexInputRef(relBuilder, filterArg, inputFieldTypes(filterArg)))
    } else {
      None
    }
  }

  /** Generate [[GeneratedAggsHandleFunction]] with the given function name and aggregate infos. */
  def generateAggsHandler(
      name: String,
      aggInfoList: AggregateInfoList): GeneratedAggsHandleFunction = {

    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val setWindowSizeCode = genSetWindowSize()
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val resetAccumulatorsCode = genResetAccumulators()
    val accumulateCode = genAccumulate()
    val retractCode = genRetract()
    val mergeCode = genMerge()
    val getValueCode = genGetValue()

    val functionName = newName(name)

    val functionCode =
      j"""
        public final class $functionName implements $AGGS_HANDLER_FUNCTION {

          ${ctx.reuseMemberCode()}

          private $STATE_DATA_VIEW_STORE store;

          public $functionName(java.lang.Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          private $RUNTIME_CONTEXT getRuntimeContext() {
            return store.getRuntimeContext();
          }

          @Override
          public void open($STATE_DATA_VIEW_STORE store) throws Exception {
            this.store = store;
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void setWindowSize(int $WINDOWS_SIZE) {
            $setWindowSizeCode
          }

          @Override
          public void accumulate($ROW_DATA $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($ROW_DATA $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge($ROW_DATA $MERGED_ACC_TERM) throws Exception {
            $mergeCode
          }

          @Override
          public void setAccumulators($ROW_DATA $ACC_TERM) throws Exception {
            $setAccumulatorsCode
          }

          @Override
          public void resetAccumulators() throws Exception {
            $resetAccumulatorsCode
          }

          @Override
          public $ROW_DATA getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $ROW_DATA createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public $ROW_DATA getValue() throws Exception {
            $getValueCode
          }

          @Override
          public void cleanup() throws Exception {
            ${ctx.reuseCleanupCode()}
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin

    new GeneratedAggsHandleFunction(
      functionName,
      functionCode,
      ctx.references.toArray,
      ctx.tableConfig)
  }

  /**
   * Generate [[GeneratedTableAggsHandleFunction]] with the given function name and aggregate infos.
   */
  def generateTableAggsHandler(
      name: String,
      aggInfoList: AggregateInfoList): GeneratedTableAggsHandleFunction = {

    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val resetAccumulatorsCode = genResetAccumulators()
    val accumulateCode = genAccumulate()
    val retractCode = genRetract()
    val mergeCode = genMerge()
    val emitValueCode = genEmitValue()

    // gen converter
    val aggExternalType = aggInfoList.getActualAggregateInfos(0).externalResultType
    val recordInputName = newName("recordInput")
    val recordToRowDataCode = genRecordToRowData(aggExternalType, recordInputName)

    val functionName = newName(name)
    val functionCode =
      j"""
        public final class $functionName implements ${className[TableAggsHandleFunction]} {

          ${ctx.reuseMemberCode()}

          private $STATE_DATA_VIEW_STORE store;

          private $CONVERT_COLLECTOR_TYPE_TERM $MEMBER_COLLECTOR_TERM;

          public $functionName(java.lang.Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
            $MEMBER_COLLECTOR_TERM = new $CONVERT_COLLECTOR_TYPE_TERM(references);
          }

          private $RUNTIME_CONTEXT getRuntimeContext() {
            return store.getRuntimeContext();
          }

          @Override
          public void open($STATE_DATA_VIEW_STORE store) throws Exception {
            this.store = store;
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void accumulate($ROW_DATA $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($ROW_DATA $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge($ROW_DATA $MERGED_ACC_TERM) throws Exception {
            $mergeCode
          }

          @Override
          public void setAccumulators($ROW_DATA $ACC_TERM) throws Exception {
            $setAccumulatorsCode
          }

          @Override
          public void resetAccumulators() throws Exception {
            $resetAccumulatorsCode
          }

          @Override
          public $ROW_DATA getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $ROW_DATA createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public void emitValue(
            $COLLECTOR<$ROW_DATA> $COLLECTOR_TERM, $ROW_DATA key, boolean isRetract)
            throws Exception {

            $MEMBER_COLLECTOR_TERM.reset(key, isRetract, $COLLECTOR_TERM);
            $emitValueCode
          }

          @Override
          public void cleanup() throws Exception {
            ${ctx.reuseCleanupCode()}
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }

          private class $CONVERT_COLLECTOR_TYPE_TERM implements $COLLECTOR {
            private $COLLECTOR<$ROW_DATA> $COLLECTOR_TERM;
            private $ROW_DATA key;
            private $JOINED_ROW result;
            private boolean isRetract = false;
            ${ctx.reuseMemberCode()}

            public $CONVERT_COLLECTOR_TYPE_TERM(java.lang.Object[] references) throws Exception {
              ${ctx.reuseInitCode()}
              result = new $JOINED_ROW();
            }

            public void reset(
              $ROW_DATA key, boolean isRetract, $COLLECTOR<$ROW_DATA> $COLLECTOR_TERM) {
              this.key = key;
              this.isRetract = isRetract;
              this.$COLLECTOR_TERM = $COLLECTOR_TERM;
            }

            public $ROW_DATA convertToRowData(Object $recordInputName) throws Exception {
              $recordToRowDataCode
            }

            @Override
            public void collect(Object $recordInputName) throws Exception {
              $ROW_DATA tempRowData = convertToRowData($recordInputName);
              result.replace(key, tempRowData);
              if (isRetract) {
                result.setRowKind($ROW_KIND.DELETE);
              } else {
                result.setRowKind($ROW_KIND.INSERT);
              }
              $COLLECTOR_TERM.collect(result);
            }

            @Override
            public void close() {
              $COLLECTOR_TERM.close();
            }
          }
        }
      """.stripMargin

    new GeneratedTableAggsHandleFunction(
      functionName,
      functionCode,
      ctx.references.toArray,
      ctx.tableConfig)
  }

  /**
   * Generate [[NamespaceAggsHandleFunction]] with the given function name and aggregate infos and
   * window properties.
   */
  def generateNamespaceAggsHandler(
      name: String,
      aggInfoList: AggregateInfoList,
      windowProperties: Seq[WindowProperty],
      sliceAssigner: SliceAssigner,
      shiftTimeZone: ZoneId): GeneratedNamespaceAggsHandleFunction[JLong] = {
    this.sliceAssignerTerm = newName("sliceAssigner")
    ctx.addReusableObjectWithName(sliceAssigner, sliceAssignerTerm)
    // we use window end timestamp to indicate a window, see SliceAssigner
    generateNamespaceAggsHandler(name, aggInfoList, windowProperties, classOf[JLong], shiftTimeZone)
  }

  /**
   * Generate [[NamespaceAggsHandleFunction]] with the given function name and aggregate infos and
   * window properties.
   */
  def generateNamespaceAggsHandler[N](
      name: String,
      aggInfoList: AggregateInfoList,
      windowProperties: Seq[WindowProperty],
      windowClass: Class[N],
      shiftTimeZone: ZoneId): GeneratedNamespaceAggsHandleFunction[N] = {

    initialWindowProperties(windowProperties, windowClass, shiftTimeZone)
    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val accumulateCode = genAccumulate()
    val retractCode = genRetract()
    val mergeCode = genMerge()
    val getValueCode = genGetValue()

    val functionName = newName(name)

    val functionCode =
      j"""
        public final class $functionName
          implements $NAMESPACE_AGGS_HANDLER_FUNCTION<$namespaceClassName> {

          ${ctx.reuseMemberCode()}

          private $STATE_DATA_VIEW_STORE store;

          private $namespaceClassName $NAMESPACE_TERM;

          public $functionName(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          private $RUNTIME_CONTEXT getRuntimeContext() {
            return store.getRuntimeContext();
          }

          @Override
          public void open($STATE_DATA_VIEW_STORE store) throws Exception {
            this.store = store;
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void accumulate($ROW_DATA $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($ROW_DATA $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge(Object ns, $ROW_DATA $MERGED_ACC_TERM) throws Exception {
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            $mergeCode
          }

          @Override
          public void setAccumulators(Object ns, $ROW_DATA $ACC_TERM)
          throws Exception {
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            $setAccumulatorsCode
          }

          @Override
          public $ROW_DATA getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $ROW_DATA createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public $ROW_DATA getValue(Object ns) throws Exception {
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            $getValueCode
          }

          @Override
          public void cleanup(Object ns) throws Exception {
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            ${ctx.reuseCleanupCode()}
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin

    new GeneratedNamespaceAggsHandleFunction[N](
      functionName,
      functionCode,
      ctx.references.toArray,
      ctx.tableConfig)
  }

  /**
   * Generate [[NamespaceTableAggsHandleFunction]] with the given function name and aggregate infos
   * and window properties.
   */
  def generateNamespaceTableAggsHandler[N](
      name: String,
      aggInfoList: AggregateInfoList,
      windowProperties: Seq[WindowProperty],
      windowClass: Class[N],
      shiftedTimeZone: ZoneId): GeneratedNamespaceTableAggsHandleFunction[N] = {

    initialWindowProperties(windowProperties, windowClass, shiftedTimeZone)
    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
    val createAccumulatorsCode = genCreateAccumulators()
    val getAccumulatorsCode = genGetAccumulators()
    val setAccumulatorsCode = genSetAccumulators()
    val accumulateCode = genAccumulate()
    val retractCode = genRetract()
    val mergeCode = genMerge()
    val emitValueCode = genEmitValue(isWindow = true)

    // gen converter
    val aggExternalType = aggInfoList.getActualAggregateInfos(0).externalResultType
    val recordInputName = newName("recordInput")
    val recordToRowDataCode = genRecordToRowData(aggExternalType, recordInputName)

    val functionName = newName(name)
    val functionCode =
      j"""
        public final class $functionName
          implements ${className[NamespaceTableAggsHandleFunction[_]]}<$namespaceClassName> {

          ${ctx.reuseMemberCode()}

          private $STATE_DATA_VIEW_STORE store;

          private $namespaceClassName $NAMESPACE_TERM;

          private $CONVERT_COLLECTOR_TYPE_TERM $MEMBER_COLLECTOR_TERM;

          public $functionName(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
            $MEMBER_COLLECTOR_TERM = new $CONVERT_COLLECTOR_TYPE_TERM(references);
          }

          private $RUNTIME_CONTEXT getRuntimeContext() {
            return store.getRuntimeContext();
          }

          @Override
          public void open($STATE_DATA_VIEW_STORE store) throws Exception {
            this.store = store;
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void accumulate($ROW_DATA $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($ROW_DATA $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge(Object ns, $ROW_DATA $MERGED_ACC_TERM) throws Exception {
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            $mergeCode
          }

          @Override
          public void setAccumulators(Object ns, $ROW_DATA $ACC_TERM)
          throws Exception {
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            $setAccumulatorsCode
          }

          @Override
          public $ROW_DATA getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $ROW_DATA createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public void emitValue(Object ns, $ROW_DATA $KEY_TERM,
            $COLLECTOR<$ROW_DATA> $COLLECTOR_TERM) throws Exception {

            $MEMBER_COLLECTOR_TERM.$COLLECTOR_TERM = $COLLECTOR_TERM;
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            $emitValueCode
          }

          @Override
          public void cleanup(Object ns) throws Exception {
            $NAMESPACE_TERM = ($namespaceClassName) ns;
            ${ctx.reuseCleanupCode()}
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }

          private class $CONVERT_COLLECTOR_TYPE_TERM implements $COLLECTOR {
            public $COLLECTOR<$ROW_DATA> $COLLECTOR_TERM;
            private $ROW_DATA timeProperties;
            private $ROW_DATA key;
            private $JOINED_ROW outerResult;
            private $JOINED_ROW innerResult;
            ${ctx.reuseMemberCode()}

            public $CONVERT_COLLECTOR_TYPE_TERM(java.lang.Object[] references) throws Exception {
              ${ctx.reuseInitCode()}
              outerResult = new $JOINED_ROW();
              innerResult = new $JOINED_ROW();
            }

            public void reset($ROW_DATA $KEY_TERM, $ROW_DATA timeProperties) {
              this.timeProperties = timeProperties;
              this.key = $KEY_TERM;
            }

            public $ROW_DATA convertToRowData(Object $recordInputName) throws Exception {
              $recordToRowDataCode
            }

            @Override
            public void collect(Object $recordInputName) throws Exception {
              $ROW_DATA tempRowData = convertToRowData($recordInputName);
              innerResult.replace(tempRowData, timeProperties);
              outerResult.replace(key, innerResult);
              $COLLECTOR_TERM.collect(outerResult);
            }

            @Override
            public void close() {
              $COLLECTOR_TERM.close();
            }
          }
        }
      """.stripMargin

    new GeneratedNamespaceTableAggsHandleFunction[N](
      functionName,
      functionCode,
      ctx.references.toArray,
      ctx.tableConfig)
  }

  private def genSetWindowSize(): String = {
    // The generated method 'setWindowSize' in OverWindowFrame#prepare will always be called
    // no matter window size is needed or not. If window size is not needed,
    // the method 'setWindowSize' will do nothing.
    // So, please make sure to set the variable 'isWindowSizeNeeded' = true
    // if window size is needed.
    if (isWindowSizeNeeded) {
      val methodName = "setWindowSize"
      ctx.startNewLocalVariableStatement(methodName)

      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
        .bindInput(DataTypes.INT().getLogicalType, WINDOWS_SIZE)
      val body = aggBufferCodeGens
        // ignore distinct agg codegen
        .filter(agg => !agg.isInstanceOf[DistinctAggCodeGen])
        .map(_.setWindowSize(exprGenerator))
        .mkString("\n")

      s"""
         |${ctx.reuseLocalVariableCode(methodName)}
         |${ctx.reuseInputUnboxingCode(WINDOWS_SIZE)}
         |${ctx.reusePerRecordCode()}
         |$body
         |""".stripMargin
    } else {
      ""
    }
  }

  private def genCreateAccumulators(): String = {
    val methodName = "createAccumulators"
    ctx.startNewLocalVariableStatement(methodName)

    // not need to bind input for ExprCodeGenerator
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
    val initAccExprs = aggBufferCodeGens.flatMap(_.createAccumulator(exprGenerator))
    val accTerm = newName("acc")
    val resultExpr = exprGenerator.generateResultExpression(
      initAccExprs,
      accTypeInfo,
      classOf[GenericRowData],
      outRow = accTerm,
      reusedOutRow = false)

    s"""
       |${ctx.reuseLocalVariableCode(methodName)}
       |${ctx.reusePerRecordCode()}
       |${resultExpr.code}
       |return ${resultExpr.resultTerm};
    """.stripMargin
  }

  private def genGetAccumulators(): String = {
    val methodName = "getAccumulators"
    ctx.startNewLocalVariableStatement(methodName)

    // no need to bind input
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
    val accExprs = aggBufferCodeGens.flatMap(_.getAccumulator(exprGenerator))
    val accTerm = newName("acc")
    // always create a new accumulator row
    val resultExpr = exprGenerator.generateResultExpression(
      accExprs,
      accTypeInfo,
      classOf[GenericRowData],
      outRow = accTerm,
      reusedOutRow = false)

    s"""
       |${ctx.reuseLocalVariableCode(methodName)}
       |${resultExpr.code}
       |return ${resultExpr.resultTerm};
    """.stripMargin
  }

  private def genSetAccumulators(): String = {
    val methodName = "setAccumulators"
    ctx.startNewLocalVariableStatement(methodName)

    // bind input1 as accumulators
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
      .bindInput(accTypeInfo, inputTerm = ACC_TERM)
    val body = aggBufferCodeGens.map(_.setAccumulator(exprGenerator)).mkString("\n")

    s"""
       |${ctx.reuseLocalVariableCode(methodName)}
       |${ctx.reuseInputUnboxingCode(ACC_TERM)}
       |$body
    """.stripMargin
  }

  private def genResetAccumulators(): String = {
    val methodName = "resetAccumulators"
    ctx.startNewLocalVariableStatement(methodName)

    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
    val body = aggBufferCodeGens.map(_.resetAccumulator(exprGenerator)).mkString("\n")

    s"""
       |${ctx.reuseLocalVariableCode(methodName)}
       |$body
    """.stripMargin
  }

  private def genAccumulate(): String = {
    if (isAccumulateNeeded) {
      // validation check
      checkNeededMethods(needAccumulate = true)

      val methodName = "accumulate"
      ctx.startNewLocalVariableStatement(methodName)

      // bind input1 as inputRow
      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
        .bindInput(inputType, inputTerm = ACCUMULATE_INPUT_TERM)
      val body = aggActionCodeGens.map(_.accumulate(exprGenerator)).mkString("\n")
      s"""
         |${ctx.reuseLocalVariableCode(methodName)}
         |${ctx.reuseInputUnboxingCode(ACCUMULATE_INPUT_TERM)}
         |${ctx.reusePerRecordCode()}
         |$body
         |""".stripMargin
    } else {
      genThrowException(
        "This function not require accumulate method, but the accumulate method is called.")
    }
  }

  private def genRetract(): String = {
    if (isRetractNeeded) {
      // validation check
      checkNeededMethods(needRetract = true)

      val methodName = "retract"
      ctx.startNewLocalVariableStatement(methodName)

      // bind input1 as inputRow
      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
        .bindInput(inputType, inputTerm = RETRACT_INPUT_TERM)
      val body = aggActionCodeGens.map(_.retract(exprGenerator)).mkString("\n")
      s"""
         |${ctx.reuseLocalVariableCode(methodName)}
         |${ctx.reuseInputUnboxingCode(RETRACT_INPUT_TERM)}
         |${ctx.reusePerRecordCode()}
         |$body
      """.stripMargin
    } else {
      genThrowException(
        "This function not require retract method, but the retract method is called.")
    }
  }

  private def genMerge(): String = {
    if (isMergeNeeded) {
      // validation check
      checkNeededMethods(needMerge = true)

      val methodName = "merge"
      ctx.startNewLocalVariableStatement(methodName)

      // the mergedAcc is partial of mergedInput, such as <key, acc> in local-global, ignore keys
      val internalAccTypes = mergedAccExternalTypes.map(fromDataTypeToLogicalType)
      val mergedAccType = if (mergedAccOffset > 0) {
        // concat padding types and acc types, use int type as padding
        // the padding types will be ignored
        val padding = Array.range(0, mergedAccOffset).map(_ => new IntType())
        RowType.of(padding ++ internalAccTypes: _*)
      } else {
        RowType.of(internalAccTypes: _*)
      }

      // bind input1 as otherAcc
      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
        .bindInput(mergedAccType, inputTerm = MERGED_ACC_TERM)
      val body = aggActionCodeGens.map(_.merge(exprGenerator)).mkString("\n")
      s"""
         |${ctx.reuseLocalVariableCode(methodName)}
         |${ctx.reuseInputUnboxingCode(MERGED_ACC_TERM)}
         |${ctx.reusePerRecordCode()}
         |$body
      """.stripMargin
    } else {
      genThrowException("This function not require merge method, but the merge method is called.")
    }
  }

  private def getWindowExpressions(
      windowProperties: Seq[WindowProperty]): Seq[GeneratedExpression] = {
    if (namespaceClassName.equals(classOf[JLong].getCanonicalName)) {
      // slicing optimization, we are using window end timestamp to indicate a window
      windowProperties.map {
        case w: WindowStart =>
          // return a Timestamp(Internal is TimestampData)
          GeneratedExpression(
            s"$TIMESTAMP_DATA.fromEpochMillis($sliceAssignerTerm.getWindowStart($NAMESPACE_TERM))",
            "false",
            "",
            w.getResultType)
        case w: WindowEnd =>
          // return a Timestamp(Internal is TimestampData)
          GeneratedExpression(
            s"$TIMESTAMP_DATA.fromEpochMillis($NAMESPACE_TERM)",
            "false",
            "",
            w.getResultType)
        case r: RowtimeAttribute =>
          // return a rowtime, use TimestampData as internal type
          GeneratedExpression(
            s"""
               |$TIMESTAMP_DATA.fromEpochMillis(
               |${getShiftEpochMills(s"$NAMESPACE_TERM - 1")})
                """.stripMargin,
            "false",
            "",
            r.getResultType
          )
        case p: ProctimeAttribute =>
          // ignore this property, it will be null at the position later
          GeneratedExpression(s"$TIMESTAMP_DATA.fromEpochMillis(-1L)", "true", "", p.getResultType)
      }
    } else {
      windowProperties.map {
        case w: WindowStart =>
          // return a Timestamp(Internal is TimestampData)
          GeneratedExpression(
            s"$TIMESTAMP_DATA.fromEpochMillis($NAMESPACE_TERM.getStart())",
            "false",
            "",
            w.getResultType)
        case w: WindowEnd =>
          // return a Timestamp(Internal is TimestampData)
          GeneratedExpression(
            s"$TIMESTAMP_DATA.fromEpochMillis($NAMESPACE_TERM.getEnd())",
            "false",
            "",
            w.getResultType)
        case r: RowtimeAttribute =>
          // return a rowtime, use TimestampData as internal type
          GeneratedExpression(
            s"""
               |$TIMESTAMP_DATA.fromEpochMillis(
               |${getShiftEpochMills(s"$NAMESPACE_TERM.getEnd() - 1")})
                """.stripMargin,
            "false",
            "",
            r.getResultType
          )
        case p: ProctimeAttribute =>
          // ignore this property, it will be null at the position later
          GeneratedExpression(s"$TIMESTAMP_DATA.fromEpochMillis(-1L)", "true", "", p.getResultType)
      }
    }
  }

  private def getShiftEpochMills(itemExpr: String): String = {
    if ("UTC".equals(shiftTimeZone.getId)) {
      itemExpr
    } else {
      val timeZoneId = ctx.addReusableShiftTimeZone(shiftTimeZone)
      s"""
         |$TIME_WINDOW_UTIL.toEpochMills($itemExpr, $timeZoneId)
          """.stripMargin
    }
  }

  private def genGetValue(): String = {
    val methodName = "getValue"
    ctx.startNewLocalVariableStatement(methodName)

    // no need to bind input
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)

    var valueExprs = aggBufferCodeGens.zipWithIndex
      .filter {
        case (_, index) =>
          // ignore the count1 agg codegen and distinct agg codegen
          ignoreAggValues.isEmpty || !ignoreAggValues.contains(index)
      }
      .map {
        case (codegen, _) =>
          codegen.getValue(exprGenerator)
      }

    if (hasNamespace) {
      // append window property results
      val windowExprs = getWindowExpressions(windowProperties)
      valueExprs = valueExprs ++ windowExprs
    }

    val aggValueTerm = newName("aggValue")
    valueType = RowType.of(valueExprs.map(_.resultType): _*)

    // always create a new result row
    val resultExpr = exprGenerator.generateResultExpression(
      valueExprs,
      valueType,
      classOf[GenericRowData],
      outRow = aggValueTerm,
      reusedOutRow = false)

    s"""
       |${ctx.reuseLocalVariableCode(methodName)}
       |${resultExpr.code}
       |return ${resultExpr.resultTerm};
    """.stripMargin
  }

  private def genEmitValue(isWindow: Boolean = false): String = {
    // validation check
    checkNeededMethods(needEmitValue = true)
    val methodName = "emitValue"
    ctx.startNewLocalVariableStatement(methodName)

    val windowCode =
      if (isWindow) {
        // no need to bind input
        val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
        val valueExprs = getWindowExpressions(windowProperties)

        val aggValueTerm = newName("windowProperties")
        valueType = RowType.of(valueExprs.map(_.resultType): _*)

        // always create a new result row
        val resultExpr = exprGenerator.generateResultExpression(
          valueExprs,
          valueType,
          classOf[GenericRowData],
          outRow = aggValueTerm,
          reusedOutRow = false)

        s"""
           |${ctx.reuseLocalVariableCode(methodName)}
           |${resultExpr.code}
           |$MEMBER_COLLECTOR_TERM.reset($KEY_TERM, ${resultExpr.resultTerm});
        """.stripMargin
      } else {
        ""
      }

    windowCode + aggBufferCodeGens(0).asInstanceOf[ImperativeAggCodeGen].emitValue
  }

  private def genRecordToRowData(aggExternalType: DataType, recordInputName: String): String = {
    val resultType = fromDataTypeToLogicalType(aggExternalType)
    val resultRowType = LogicalTypeUtils.toRowType(resultType)

    val newCtx = new CodeGeneratorContext(ctx.tableConfig, ctx.classLoader)
    val exprGenerator = new ExprCodeGenerator(newCtx, false).bindInput(resultType)
    val resultExpr = exprGenerator.generateConverterResultExpression(
      resultRowType,
      classOf[GenericRowData],
      "convertResult")

    val converterCode = CodeGenUtils.genToInternalConverter(ctx, aggExternalType, recordInputName)
    val resultTypeClass = boxedTypeTermForType(resultType)
    s"""
       |${newCtx.reuseMemberCode()}
       |$resultTypeClass ${exprGenerator.input1Term} = ($resultTypeClass) $converterCode;
       |${newCtx.reuseLocalVariableCode()}
       |${newCtx.reuseInputUnboxingCode()}
       |${resultExpr.code}
       |return ${resultExpr.resultTerm};
       """.stripMargin
  }

  private def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false,
      needEmitValue: Boolean = false): Unit = {
    // check and validate the needed methods
    aggBufferCodeGens.foreach(
      _.checkNeededMethods(needAccumulate, needRetract, needMerge, needReset, needEmitValue))
  }

  private def genThrowException(msg: String): String = {
    s"""
       |throw new java.lang.RuntimeException("$msg");
     """.stripMargin
  }
}

object AggsHandlerCodeGenerator {

  /** static terms * */
  val ACC_TERM = "acc"
  val MERGED_ACC_TERM = "otherAcc"
  val ACCUMULATE_INPUT_TERM = "accInput"
  val RETRACT_INPUT_TERM = "retractInput"
  val WINDOWS_SIZE = "windowSize"
  val DISTINCT_KEY_TERM = "distinctKey"

  val NAMESPACE_TERM = "namespace"
  val STORE_TERM = "store"

  val COLLECTOR: String = className[Collector[_]]
  val COLLECTOR_TERM = "out"
  val MEMBER_COLLECTOR_TERM = "convertCollector"
  val CONVERT_COLLECTOR_TYPE_TERM = "ConvertCollector"
  val KEY_TERM = "groupKey"

  val INPUT_NOT_NULL = false

  /**
   * Create DataView term, for example, acc1_map_dataview.
   *
   * @return
   *   term to access MapView or ListView
   */
  def createDataViewTerm(spec: DataViewSpec): String = {
    s"${spec.getStateId}_dataview"
  }

  /** Creates RawValueData term which wraps the specific DataView term. */
  def createDataViewRawValueTerm(spec: DataViewSpec): String = {
    s"${createDataViewTerm(spec)}_raw_value"
  }

  /**
   * Create DataView backup term, for example, acc1_map_dataview_backup. The backup dataview term is
   * used for merging two statebackend dataviews, e.g. session window.
   *
   * @return
   *   term to access backup MapView or ListView
   */
  def createDataViewBackupTerm(spec: DataViewSpec): String = {
    s"${spec.getStateId}_dataview_backup"
  }

  /** Creates RawValueData term which wraps the specific DataView backup term. */
  def createDataViewBackupRawValueTerm(spec: DataViewSpec): String = {
    s"${createDataViewBackupTerm(spec)}_raw_value"
  }

  def addReusableStateDataViews(
      ctx: CodeGeneratorContext,
      viewSpecs: Array[DataViewSpec],
      hasNamespace: Boolean,
      enableBackupDataView: Boolean): Unit = {
    // add reusable dataviews to context
    viewSpecs.foreach {
      spec =>
        val stateId = '"' + spec.getStateId + '"'
        val (viewTypeTerm, stateStoreCall) = spec match {

          case spec: ListViewSpec =>
            val viewTypeTerm = className[StateListView[_, _]]
            val elementSerializerTerm = addReusableDataViewSerializer(
              ctx,
              spec.getElementSerializer,
              () => spec.getElementDataType)
            val stateStoreCall =
              s"getStateListView($stateId, $elementSerializerTerm)"
            (viewTypeTerm, stateStoreCall)

          case spec: MapViewSpec =>
            val viewTypeTerm = className[StateMapView[_, _, _]]
            val withNullKey = spec.containsNullKey()
            val keySerializerTerm = addReusableDataViewSerializer(
              ctx,
              spec.getKeySerializer,
              () => spec.getKeyDataType
            )
            val valueSerializerTerm = addReusableDataViewSerializer(
              ctx,
              spec.getValueSerializer,
              () => spec.getValueDataType)
            val stateStoreCall =
              s"getStateMapView($stateId, $withNullKey, $keySerializerTerm, $valueSerializerTerm)"
            (viewTypeTerm, stateStoreCall)
        }

        val viewFieldTerm = createDataViewTerm(spec)
        val viewFieldInternalTerm = createDataViewRawValueTerm(spec)

        ctx.addReusableMember(s"private $viewTypeTerm $viewFieldTerm;")
        ctx.addReusableMember(s"private $BINARY_RAW_VALUE $viewFieldInternalTerm;")

        val openCode =
          s"""
             |$viewFieldTerm = ($viewTypeTerm) $STORE_TERM.$stateStoreCall;
             |$viewFieldInternalTerm = $BINARY_RAW_VALUE.fromObject($viewFieldTerm);
         """.stripMargin
        ctx.addReusableOpenStatement(openCode)

        // only cleanup dataview term, do not need to cleanup backup
        val cleanupCode = if (hasNamespace) {
          s"""
             |$viewFieldTerm.setCurrentNamespace($NAMESPACE_TERM);
             |$viewFieldTerm.clear();
        """.stripMargin
        } else {
          s"""
             |$viewFieldTerm.clear();
        """.stripMargin
        }
        ctx.addReusableCleanupStatement(cleanupCode)

        // generate backup dataview codes
        if (enableBackupDataView) {
          val backupViewTerm = createDataViewBackupTerm(spec)
          val backupViewInternalTerm = createDataViewBackupRawValueTerm(spec)
          // create backup dataview
          ctx.addReusableMember(s"private $viewTypeTerm $backupViewTerm;")
          ctx.addReusableMember(s"private $BINARY_RAW_VALUE $backupViewInternalTerm;")
          val backupOpenCode =
            s"""
               |$backupViewTerm = ($viewTypeTerm) $STORE_TERM.$stateStoreCall;
               |$backupViewInternalTerm = $BINARY_RAW_VALUE.fromObject($backupViewTerm);
           """.stripMargin
          ctx.addReusableOpenStatement(backupOpenCode)
        }
    }
  }

  private def addReusableDataViewSerializer(
      ctx: CodeGeneratorContext,
      legacySerializer: Optional[TypeSerializer[_]],
      dataType: () => DataType): String = {
    toScala(legacySerializer) match {
      case Some(serializer) =>
        ctx.addReusableObject(serializer, "serializer")
      case None =>
        ctx.addReusableExternalSerializer(dataType())
    }
  }
}
