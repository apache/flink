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
package org.apache.flink.table.codegen.agg

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.`type`.{InternalType, InternalTypes, RowType}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.codegen.CodeGenUtils.{BASE_ROW, _}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.dataview._
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.generated.{AggsHandleFunction, GeneratedAggsHandleFunction, GeneratedNamespaceAggsHandleFunction, NamespaceAggsHandleFunction}
import org.apache.flink.table.plan.util.AggregateInfoList
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder

/**
  * A code generator for generating [[AggsHandleFunction]].
  *
  * @param copyInputField copy input field element if true (only mutable type will be copied),
  *                       set to true if field will be buffered (such as local aggregate)
  */
class AggsHandlerCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    inputFieldTypes: Seq[InternalType],
    needRetract: Boolean,
    copyInputField: Boolean,
    needAccumulate: Boolean = true) {

  private val inputType = new RowType(inputFieldTypes: _*)

  /** constant expressions that act like a second input in the parameter indices. */
  private var constantExprs: Seq[GeneratedExpression] = Seq()

  /** window properties like window_start and window_end, only used in window aggregates */
  private var namespaceClassName: String = _
  private var windowProperties: Seq[WindowProperty] = Seq()
  private var hasNamespace: Boolean = false

  /** Aggregates informations */
  private var accTypeInfo: RowType = _
  private var aggBufferSize: Int = _

  private var mergedAccExternalTypes: Array[TypeInformation[_]] = _
  private var mergedAccOffset: Int = 0
  private var mergedAccOnHeap: Boolean = false

  private var ignoreAggValues: Array[Int] = Array()

  private var needMerge = false

  var valueType: RowType = _

  /**
    * The [[aggBufferCodeGens]] and [[aggActionCodeGens]] will be both created when code generate
    * an [[AggsHandleFunction]] or [[NamespaceAggsHandleFunction]]. They both contain all the
    * same AggCodeGens, but are different in the organizational form. The [[aggBufferCodeGens]]
    * flatten all the AggCodeGens in a flat format. The [[aggActionCodeGens]] organize all the
    * AggCodeGens in a tree format. If there is no distinct aggregate, the [[aggBufferCodeGens]]
    * and [[aggActionCodeGens]] are totally the same.
    *
    * When different aggregate distinct on the same field but on different filter conditions,
    * they will share the same distinct state, see DistinctAggCodeGen.DistinctValueGenerator
    * for more information.
    */

  /**
    * The aggBufferCodeGens is organized according to the agg buffer order, which is in a flat
    * format, and is only used to generate the methods relative to accumulators, Such as
    * [[genCreateAccumulators()]], [[genGetAccumulators()]], [[genSetAccumulators()]].
    *
    * For example if we have :
    * count(*), count(distinct a), count(distinct a) filter d > 5, sum(a), sum(distinct a)
    *
    * then the members of aggBufferCodeGens are organized looks like this:
    * +----------+-----------+-----------+---------+---------+----------------+
    * | count(*) | count(a') | count(a') |  sum(a) | sum(a') | distinct(a) a' |
    * +----------+-----------+-----------+---------+---------+----------------+
    * */
  private var aggBufferCodeGens: Array[AggCodeGen] = _

  /**
    * The aggActionCodeGens is organized according to the aggregate calling order, which is in
    * a tree format. Such as the aggregates distinct on the same fields should be accumulated
    * together when distinct is satisfied. And this is only used to generate the methods relative
    * to aggregate action. Such as [[genAccumulate()]], [[genRetract()]], [[genMerge()]].
    *
    * For example if we have :
    * count(*), count(distinct a), count(distinct a) filter d > 5, sum(a), sum(distinct a)
    *
    * then the members of aggActionCodeGens are organized looks like this:
    *
    * +----------------------------------------------------+
    * | count(*) | sum(a) | distinct(a) a'                 |
    * |          |        |   |-- count(a')                |
    * |          |        |   |-- count(a') (filter d > 5) |
    * |          |        |   |-- sum(a')                  |
    * +----------------------------------------------------+
    */
  private var aggActionCodeGens: Array[AggCodeGen] = _

  /**
    * Adds constant expressions that act like a second input in the parameter indices.
    */
  def withConstants(literals: Seq[RexLiteral]): AggsHandlerCodeGenerator = {
    // create constants
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
    val exprs = literals.map(exprGenerator.generateExpression)
    this.constantExprs = exprs.map(ctx.addReusableConstant(_, nullCheck = true))
    this
  }

  /**
    * Sets merged accumulator information.
    *
    * @param mergedAccOffset the mergedAcc may come from local aggregate,
    *                         this is the first buffer offset in the row
    * @param mergedAccOnHeap true if the mergedAcc is on heap, otherwise
    * @param mergedAccExternalTypes the merged acc types
    */
  def withMerging(
      mergedAccOffset: Int,
      mergedAccOnHeap: Boolean,
      mergedAccExternalTypes: Array[TypeInformation[_]] = null): AggsHandlerCodeGenerator = {
    this.mergedAccOffset = mergedAccOffset
    this.mergedAccOnHeap = mergedAccOnHeap
    this.mergedAccExternalTypes = mergedAccExternalTypes
    this.needMerge = true
    this
  }

  /**
    * Adds window properties such as window_start, window_end
    */
  private def initialWindowProperties(
      windowProperties: Seq[WindowProperty],
      windowClass: Class[_]): Unit = {
    this.windowProperties = windowProperties
    this.namespaceClassName = windowClass.getCanonicalName
    this.hasNamespace = true
  }

  /**
    * Adds aggregate infos into context
    */
  private def initialAggregateInformation(aggInfoList: AggregateInfoList): Unit = {

    this.accTypeInfo = new RowType(
      aggInfoList.getAccTypes.map(createInternalTypeFromTypeInfo): _*)
    this.aggBufferSize = accTypeInfo.getArity
    var aggBufferOffset: Int = 0

    if (mergedAccExternalTypes == null) {
      mergedAccExternalTypes = aggInfoList.getAccTypes
    }

    val aggCodeGens = aggInfoList.aggInfos.map { aggInfo =>
      val filterExpr = createFilterExpression(
        aggInfo.agg.filterArg,
        aggInfo.aggIndex,
        aggInfo.agg.name)

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
            constantExprs,
            relBuilder)
        case _: AggregateFunction[_, _] =>
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
          mergedAccOffset,
          aggBufferOffset,
          aggBufferSize,
          hasNamespace,
          needMerge,
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

  /**
    * Creates filter argument access expression, none if no filter
    */
  private def createFilterExpression(
      filterArg: Int,
      aggIndex: Int,
      aggName: String): Option[Expression] = {

    if (filterArg > 0) {
      val name = s"agg_${aggIndex}_filter"
      val filterType = inputFieldTypes(filterArg)
      if (filterType != InternalTypes.BOOLEAN) {
        throw new TableException(s"filter arg must be boolean, but is $filterType, " +
            s"the aggregate is $aggName.")
      }
      Some(new ResolvedAggInputReference(name, filterArg, inputFieldTypes(filterArg)))
    } else {
      None
    }
  }

  /**
    * Generate [[GeneratedAggsHandleFunction]] with the given function name and aggregate infos.
    */
  def generateAggsHandler(
      name: String,
      aggInfoList: AggregateInfoList): GeneratedAggsHandleFunction = {

    initialAggregateInformation(aggInfoList)

    // generates all methods body first to add necessary reuse code to context
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

          public $functionName(java.lang.Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public void open($STATE_DATA_VIEW_STORE store) throws Exception {
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void accumulate($BASE_ROW $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($BASE_ROW $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge($BASE_ROW $MERGED_ACC_TERM) throws Exception {
            $mergeCode
          }

          @Override
          public void setAccumulators($BASE_ROW $ACC_TERM) throws Exception {
            $setAccumulatorsCode
          }

          @Override
          public void resetAccumulators() throws Exception {
            $resetAccumulatorsCode
          }

          @Override
          public $BASE_ROW getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $BASE_ROW createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public $BASE_ROW getValue() throws Exception {
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

    new GeneratedAggsHandleFunction(functionName, functionCode, ctx.references.toArray)
  }

  /**
    * Generate [[GeneratedAggsHandleFunction]] with the given function name and aggregate infos
    * and window properties.
    */
  def generateNamespaceAggsHandler[N](
      name: String,
      aggInfoList: AggregateInfoList,
      windowProperties: Seq[WindowProperty],
      windowClass: Class[N]): GeneratedNamespaceAggsHandleFunction[N] = {

    initialWindowProperties(windowProperties, windowClass)
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

          public $functionName(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public void open($STATE_DATA_VIEW_STORE store) throws Exception {
            ${ctx.reuseOpenCode()}
          }

          @Override
          public void accumulate($BASE_ROW $ACCUMULATE_INPUT_TERM) throws Exception {
            $accumulateCode
          }

          @Override
          public void retract($BASE_ROW $RETRACT_INPUT_TERM) throws Exception {
            $retractCode
          }

          @Override
          public void merge(Object ns, $BASE_ROW $MERGED_ACC_TERM) throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            $mergeCode
          }

          @Override
          public void setAccumulators(Object ns, $BASE_ROW $ACC_TERM)
          throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            $setAccumulatorsCode
          }

          @Override
          public $BASE_ROW getAccumulators() throws Exception {
            $getAccumulatorsCode
          }

          @Override
          public $BASE_ROW createAccumulators() throws Exception {
            $createAccumulatorsCode
          }

          @Override
          public $BASE_ROW getValue(Object ns) throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            $getValueCode
          }

          @Override
          public void cleanup(Object ns) throws Exception {
            $namespaceClassName $NAMESPACE_TERM = ($namespaceClassName) ns;
            ${ctx.reuseCleanupCode()}
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin

    new GeneratedNamespaceAggsHandleFunction[N](functionName, functionCode, ctx.references.toArray)
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
      classOf[GenericRow],
      outRow = accTerm,
      reusedOutRow = false)

    s"""
       |${ctx.reuseLocalVariableCode(methodName)}
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
      classOf[GenericRow],
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
    if (needAccumulate) {
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
         |$body
         |""".stripMargin
    } else {
      genThrowException(
        "This function not require accumulate method, but the accumulate method is called.")
    }
  }

  private def genRetract(): String = {
    if (needRetract) {
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
         |$body
      """.stripMargin
    } else {
      genThrowException(
        "This function not require retract method, but the retract method is called.")
    }
  }

  private def genMerge(): String = {
    if (needMerge) {
      // validation check
      checkNeededMethods(needMerge = true)

      val methodName = "merge"
      ctx.startNewLocalVariableStatement(methodName)

      // the mergedAcc is partial of mergedInput, such as <key, acc> in local-global, ignore keys
      val internalAccTypes = mergedAccExternalTypes.map(createInternalTypeFromTypeInfo)
      val mergedAccType = if (mergedAccOffset > 0) {
        // concat padding types and acc types, use int type as padding
        // the padding types will be ignored
        val padding = Array.range(0, mergedAccOffset).map(_ => InternalTypes.INT)
        new RowType(padding ++ internalAccTypes: _*)
      } else {
        new RowType(internalAccTypes: _*)
      }

      // bind input1 as otherAcc
      val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
          .bindInput(mergedAccType, inputTerm = MERGED_ACC_TERM)
      val body = aggActionCodeGens.map(_.merge(exprGenerator)).mkString("\n")
      s"""
         |${ctx.reuseLocalVariableCode(methodName)}
         |${ctx.reuseInputUnboxingCode(MERGED_ACC_TERM)}
         |$body
      """.stripMargin
    } else {
      genThrowException(
        "This function not require merge method, but the merge method is called.")
    }
  }

  private def genGetValue(): String = {
    val methodName = "getValue"
    ctx.startNewLocalVariableStatement(methodName)

    // no need to bind input
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)

    var valueExprs = aggBufferCodeGens.zipWithIndex.filter { case (_, index) =>
      // ignore the count1 agg codegen and distinct agg codegen
      ignoreAggValues.isEmpty || !ignoreAggValues.contains(index)
    }.map { case (codegen, _) =>
      codegen.getValue(exprGenerator)
    }

    if (hasNamespace) {
      // append window property results
      val windowExprs = windowProperties.map {
        case w: WindowStart =>
          // return a Timestamp(Internal is long)
          GeneratedExpression(
            s"$NAMESPACE_TERM.getStart()", "false", "", w.resultType)
        case w: WindowEnd =>
          // return a Timestamp(Internal is long)
          GeneratedExpression(
            s"$NAMESPACE_TERM.getEnd()", "false", "", w.resultType)
//        case r: RowtimeAttribute =>
//          // return a rowtime, use long as internal type
//          GeneratedExpression(
//            s"$NAMESPACE_TERM.getEnd() - 1", "false", "", r.resultType.toInternalType)
//        case p: ProctimeAttribute =>
//          // ignore this property, it will be null at the position later
//          GeneratedExpression("-1L", "true", "", p.resultType.toInternalType)
      }
      valueExprs = valueExprs ++ windowExprs
    }

    val aggValueTerm = newName("aggValue")
    valueType = new RowType(valueExprs.map(_.resultType): _*)

    // always create a new result row
    val resultExpr = exprGenerator.generateResultExpression(
      valueExprs,
      valueType,
      classOf[GenericRow],
      outRow = aggValueTerm,
      reusedOutRow = false)

    s"""
       |${ctx.reuseLocalVariableCode(methodName)}
       |${resultExpr.code}
       |return ${resultExpr.resultTerm};
    """.stripMargin
  }

  private def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false): Unit = {
    // check and validate the needed methods
    aggBufferCodeGens
        .foreach(_.checkNeededMethods(needAccumulate, needRetract, needMerge, needReset))
  }

  private def genThrowException(msg: String): String = {
    s"""
       |throw new java.lang.RuntimeException("$msg");
     """.stripMargin
  }
}

object AggsHandlerCodeGenerator {

  /** static terms **/
  val ACC_TERM = "acc"
  val MERGED_ACC_TERM = "otherAcc"
  val ACCUMULATE_INPUT_TERM = "accInput"
  val RETRACT_INPUT_TERM = "retractInput"
  val DISTINCT_KEY_TERM = "distinctKey"

  val NAMESPACE_TERM = "namespace"
  val STORE_TERM = "store"

  val INPUT_NOT_NULL = false

  /**
    * Create DataView term, for example, acc1_map_dataview.
    *
    * @return term to access MapView or ListView
    */
  def createDataViewTerm(spec: DataViewSpec): String = {
    s"${spec.stateId}_dataview"
  }

  /**
    * Creates BinaryGeneric term which wraps the specific DataView term.
    */
  def createDataViewBinaryGenericTerm(spec: DataViewSpec): String = {
    s"${createDataViewTerm(spec)}_binary_generic"
  }

  /**
    * Create DataView backup term, for example, acc1_map_dataview_backup.
    * The backup dataview term is used for merging two statebackend
    * dataviews, e.g. session window.
    *
    * @return term to access backup MapView or ListView
    */
  def createDataViewBackupTerm(spec: DataViewSpec): String = {
    s"${spec.stateId}_dataview_backup"
  }

  /**
    * Creates BinaryGeneric term which wraps the specific DataView backup term.
    */
  def createDataViewBackupBinaryGenericTerm(spec: DataViewSpec): String = {
    s"${createDataViewBackupTerm(spec)}_binary_generic"
  }

  def addReusableStateDataViews(
      ctx: CodeGeneratorContext,
      viewSpecs: Array[DataViewSpec],
      hasNamespace: Boolean,
      enableBackupDataView: Boolean): Unit = {
    // add reusable dataviews to context
    viewSpecs.foreach { spec =>
      val (viewTypeTerm, registerCall) = spec match {
        case ListViewSpec(_, _, _) => (className[StateListView[_, _]], "getStateListView")
        case MapViewSpec(_, _, _) => (className[StateMapView[_, _, _]], "getStateMapView")
      }
      val viewFieldTerm = createDataViewTerm(spec)
      val viewFieldInternalTerm = createDataViewBinaryGenericTerm(spec)
      val viewTypeInfo = ctx.addReusableObject(spec.dataViewTypeInfo, "viewTypeInfo")
      val parameters = s""""${spec.stateId}", $viewTypeInfo"""

      ctx.addReusableMember(s"private $viewTypeTerm $viewFieldTerm;")
      ctx.addReusableMember(s"private $BINARY_GENERIC $viewFieldInternalTerm;")

      val openCode =
        s"""
           |$viewFieldTerm = ($viewTypeTerm) $STORE_TERM.$registerCall($parameters);
           |$viewFieldInternalTerm = ${genToInternal(ctx, spec.dataViewTypeInfo, viewFieldTerm)};
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
        val backupViewInternalTerm = createDataViewBackupBinaryGenericTerm(spec)
        // create backup dataview
        ctx.addReusableMember(s"private $viewTypeTerm $backupViewTerm;")
        ctx.addReusableMember(s"private $BINARY_GENERIC $backupViewInternalTerm;")
        val backupOpenCode =
          s"""
             |$backupViewTerm = ($viewTypeTerm) $STORE_TERM.$registerCall($parameters);
             |$backupViewInternalTerm = ${genToInternal(
                ctx, spec.dataViewTypeInfo, backupViewTerm)};
           """.stripMargin
        ctx.addReusableOpenStatement(backupOpenCode)
      }
    }
  }
}
