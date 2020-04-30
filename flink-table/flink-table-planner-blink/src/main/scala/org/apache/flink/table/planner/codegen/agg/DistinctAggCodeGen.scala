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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, _}
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateFieldAccess, generateInputAccess}
import org.apache.flink.table.planner.codegen.GeneratedExpression._
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.plan.utils.DistinctInfo
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.util.Preconditions
import org.apache.flink.util.Preconditions.checkArgument
import org.apache.calcite.tools.RelBuilder
import java.lang.{Long => JLong}

/**
  * It is for code generate distinct aggregate. The distinct aggregate buffer is a MapView which
  * is used to store the unique keys and the frequency of appearance. When a key is been seen the
  * first time, we will trigger the inner aggregate function's accumulate() function.
  *
  * @param ctx  the code gen context
  * @param distinctInfo the distinct information
  * @param distinctIndex  the index of this distinct in all distincts
  * @param innerAggCodeGens the code generator of inner aggregate
  * @param filterExpressions filter argument access expression, none if no filter
  * @param mergedAccOffset   the mergedAcc may come from local aggregate,
  *                          this is the first buffer offset in the row
  * @param aggBufferOffset   the offset in the buffers of this aggregate
  * @param aggBufferSize     the total size of aggregate buffers
  * @param hasNamespace      whether the accumulators state has namespace
  * @param mergedAccOnHeap   whether the merged accumulator is on heap, otherwise is on state
  * @param consumeRetraction whether the distinct consumes retraction
  * @param inputFieldCopy    copy input field element if true (only mutable type will be copied)
  * @param relBuilder        the rel builder to translate expressions to calcite rex nodes
  */
class DistinctAggCodeGen(
  ctx: CodeGeneratorContext,
  distinctInfo: DistinctInfo,
  distinctIndex: Int,
  innerAggCodeGens: Array[AggCodeGen],
  filterExpressions: Array[Option[Expression]],
  mergedAccOffset: Int,
  aggBufferOffset: Int,
  aggBufferSize: Int,
  hasNamespace: Boolean,
  needMerge: Boolean,
  mergedAccOnHeap: Boolean,
  consumeRetraction: Boolean,
  inputFieldCopy: Boolean,
  relBuilder: RelBuilder) extends AggCodeGen {

  val MAP_VIEW: String = className[MapView[_, _]]
  val MAP_ENTRY: String = className[java.util.Map.Entry[_, _]]
  val ITERABLE: String = className[java.lang.Iterable[_]]

  val aggCount: Int = innerAggCodeGens.length
  val externalAccType: DataType = distinctInfo.accType
  val internalAccType: LogicalType = fromDataTypeToLogicalType(externalAccType)
  val keyType: DataType = distinctInfo.keyType
  val internalKeyType: LogicalType = fromDataTypeToLogicalType(keyType)
  val keyTypeTerm: String = keyType.getConversionClass.getCanonicalName
  val distinctAccTerm: String = s"distinct_view_$distinctIndex"
  val distinctBackupAccTerm: String = s"distinct_backup_view_$distinctIndex"

  val isValueChangedTerm: String = s"is_distinct_value_changed_$distinctIndex"
  val isValueEmptyTerm: String = s"is_distinct_value_empty_$distinctIndex"
  val valueGenerator: DistinctValueGenerator = createDistinctValueGenerator()
  private val rexNodeGen = new ExpressionConverter(relBuilder)

  addReusableDistinctAccumulator()

  /**
    * Add the distinct accumulator to the member variable and open close methods.
    */
  private def addReusableDistinctAccumulator(): Unit = {
    // sanity check
    if (distinctInfo.excludeAcc) {
      // it only works in incremental mode when the distinct acc is excluded
      // the distinct mapview must works on state mode when incremental mode
      Preconditions.checkState(distinctInfo.dataViewSpec.nonEmpty)
    }

    val enableBackupDataView = needMerge && !mergedAccOnHeap

    // add state mapview to member field
    addReusableStateDataViews(
      ctx,
      distinctInfo.dataViewSpec.toArray,
      hasNamespace,
      enableBackupDataView)


    // add distinctAccTerm to member field
    ctx.addReusableMember(s"private $MAP_VIEW $distinctAccTerm;")
    if (enableBackupDataView) {
      ctx.addReusableMember(s"private $MAP_VIEW $distinctBackupAccTerm;")
    }

    // when dataview works on state, assign the stateDataView to accTerm in open method
    distinctInfo.dataViewSpec match {
      case Some(spec) =>
        val dataviewTerm = createDataViewTerm(spec)
        ctx.addReusableOpenStatement(s"$distinctAccTerm = $dataviewTerm;")
        if (enableBackupDataView) {
          val dataviewBackupTerm = createDataViewBackupTerm(spec)
          ctx.addReusableOpenStatement(s"$distinctBackupAccTerm = $dataviewBackupTerm;")
        }
      case None => // do nothing
    }
  }

  override def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    if (distinctInfo.excludeAcc) {
      // when the distinct acc is excluded, no need to create distinct accumulator
      Seq()
    } else {
      val Seq(mapViewTerm, accTerm) = newNames("mapview", "distinct_acc")
      val code =
        s"""
           |$MAP_VIEW $mapViewTerm = new $MAP_VIEW();
           |$BINARY_GENERIC $accTerm = ${genToInternal(ctx, externalAccType, mapViewTerm)};
         """.stripMargin

      Seq(GeneratedExpression(accTerm, NEVER_NULL, code, internalAccType))
    }
  }

  override def setAccumulator(generator: ExprCodeGenerator): String = {
    generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      aggBufferOffset,
      useStateDataView = true,
      useBackupDataView = false)
    // return empty because the access code is set in ctx's ReusableInputUnboxingExprs
    ""
  }

  override def resetAccumulator(generator: ExprCodeGenerator): String = {
    if (distinctInfo.excludeAcc) {
      ""
    } else {
      s"$distinctAccTerm.clear();"
    }
  }

  override def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    if (distinctInfo.excludeAcc) {
      // when the distinct acc is excluded, the accumulator result shouldn't include distinct acc
      Seq()
    } else {
      val accTerm = newName("distinct_acc")
      val code =
        s"""
           |$BINARY_GENERIC $accTerm = ${genToInternal(ctx, externalAccType, distinctAccTerm)};
         """.stripMargin

      Seq(GeneratedExpression(
        accTerm,
        NEVER_NULL,
        code,
        internalAccType))
    }
  }

  override def accumulate(generator: ExprCodeGenerator): String = {
    val keyExpr = generateKeyExpression(ctx, generator)
    val key = keyExpr.resultTerm
    val accumulateCode = innerAggCodeGens.map(_.accumulate(generator))
    val valueTerm = newName("value")
    val valueTypeTerm = valueGenerator.valueTypeTerm
    val filterResults = filterExpressions.map {
      case None => None
      case Some(f) => Some(generator.generateExpression(f.accept(rexNodeGen)).resultTerm)
    }

    val head =
      s"""
         |${keyExpr.code}
         |$valueTypeTerm $valueTerm = ($valueTypeTerm) $distinctAccTerm.get($key);
         |if ($valueTerm == null) {
         |  $valueTerm = ${valueGenerator.initialValue};
         |}
       """.stripMargin

    val body = if (consumeRetraction) {
      // input contains retraction, due to local/global, the value might be empty, and need remove
      s"""
         |$head
         |boolean $isValueEmptyTerm = true;
         |${valueGenerator.foreachAccumulate(valueTerm, accumulateCode, filterResults)}
         |if ($isValueEmptyTerm) {
         |  $distinctAccTerm.remove($key);
         |} else {
         |  $distinctAccTerm.put($key, $valueTerm);
         |}
       """.stripMargin
    } else {
      // input contains only append messages, update value only when value changed
      s"""
         |$head
         |boolean $isValueChangedTerm = false;
         |${valueGenerator.foreachAccumulate(valueTerm, accumulateCode, filterResults)}
         |if ($isValueChangedTerm) {
         |  $distinctAccTerm.put($key, $valueTerm);
         |}
       """.stripMargin
    }

    if (filterResults.forall(_.isDefined)) {
      // using the `condition` below to filter data so as to reduce state cost
      // if all distinct aggregations on same column have filter.
      val condition = filterResults.flatten.mkString(" || ")
      s"""
         |if ($condition) {
         |  $body
         |}
       """.stripMargin
    } else {
      body
    }
  }

  override def retract(generator: ExprCodeGenerator): String = {
    if (!consumeRetraction) {
      throw new TableException("This should never happen, please file a issue.")
    }
    val keyExpr = generateKeyExpression(ctx, generator)
    val key = keyExpr.resultTerm
    val retractCodes = innerAggCodeGens.map(_.retract(generator))
    val valueTerm = newName("value")
    val valueTypeTerm = valueGenerator.valueTypeTerm
    val filterResults = filterExpressions.map {
      case None => None
      case Some(f) => Some(generator.generateExpression(f.accept(rexNodeGen)).resultTerm)
    }

    val head =
      s"""
         |${keyExpr.code}
         |$valueTypeTerm $valueTerm = ($valueTypeTerm) $distinctAccTerm.get($key);
         |if ($valueTerm == null) {
         |  $valueTerm = ${valueGenerator.initialValue};
         |}
       """.stripMargin

    val body =
      s"""
         |$head
         |boolean $isValueEmptyTerm = true;
         |${valueGenerator.foreachRetract(valueTerm, retractCodes, filterResults)}
         |if ($isValueEmptyTerm) {
         |  $distinctAccTerm.remove($key);
         |} else {
         |  $distinctAccTerm.put($key, $valueTerm);
         |}
       """.stripMargin

    if (filterResults.forall(_.isDefined)) {
      // using the `condition` below to filter data so as to reduce state cost
      // if all distinct aggregations on same column have filter.
      val condition = filterResults.flatten.mkString(" || ")
      s"""
         |if ($condition) {
         |  $body
         |}
       """.stripMargin
    } else {
      body
    }
  }

  override def merge(generator: ExprCodeGenerator): String = {
    // generate other MapView acc field
    val otherAccExpr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      mergedAccOffset + aggBufferOffset,
      useStateDataView = !mergedAccOnHeap,
      useBackupDataView = true)

    val keyTerm = newName(DISTINCT_KEY_TERM)
    val exprGenerator = new ExprCodeGenerator(ctx, INPUT_NOT_NULL)
      .bindInput(internalKeyType, inputTerm = keyTerm)
    val accumulateCodes = innerAggCodeGens.map(_.accumulate(exprGenerator))
    val retractCodes = if (consumeRetraction) {
      innerAggCodeGens.map(_.retract(exprGenerator))
    } else {
      innerAggCodeGens.map(_ =>
          "throw new RuntimeException(\"This distinct aggregate do not consume retractions, " +
            "but received retract message, which should never happen.\");")
    }

    val otherAccTerm = otherAccExpr.resultTerm
    val otherEntries = newName("otherEntries")
    val valueTypeTerm = valueGenerator.valueTypeTerm
    val thisValue = "thisValue"
    val otherValue = "otherValue"

    s"""
       |$ITERABLE<$MAP_ENTRY> $otherEntries = ($ITERABLE<$MAP_ENTRY>) $otherAccTerm.entries();
       |if ($otherEntries != null) {
       |  for ($MAP_ENTRY entry: $otherEntries) {
       |    $keyTypeTerm $keyTerm = ($keyTypeTerm) entry.getKey();
       |    ${ctx.reuseInputUnboxingCode(keyTerm)}
       |    $valueTypeTerm $otherValue = ($valueTypeTerm) entry.getValue();
       |    $valueTypeTerm $thisValue = ($valueTypeTerm) $distinctAccTerm.get($keyTerm);
       |    if ($thisValue == null) {
       |      $thisValue = ${valueGenerator.initialValue};
       |    }
       |    boolean $isValueChangedTerm = false;
       |    boolean $isValueEmptyTerm = false;
       |    ${valueGenerator.foreachMerge(thisValue, otherValue, accumulateCodes, retractCodes)}
       |    if ($isValueEmptyTerm) {
       |      $distinctAccTerm.remove($keyTerm);
       |    } else if ($isValueChangedTerm) { // value is not empty and is changed, do update
       |      $distinctAccTerm.put($keyTerm, $thisValue);
       |    }
       |  } // end foreach
       |} // end otherEntries != null
   """.stripMargin
  }

  override def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    throw new TableException(
      "Distinct shouldn't return result value, this is a bug, please file a issue.")
  }

  override def checkNeededMethods(
      needAccumulate: Boolean,
      needRetract: Boolean,
      needMerge: Boolean,
      needReset: Boolean,
      needEmitValue: Boolean): Unit = {
    if (needMerge) {
      // see merge method for more information
      innerAggCodeGens
      .foreach(_.checkNeededMethods(needAccumulate = true, needRetract = consumeRetraction))
    } else {
      innerAggCodeGens.foreach(
        _.checkNeededMethods(needAccumulate, needRetract, needMerge, needReset, needEmitValue))
    }
  }

  private def generateKeyExpression(
      ctx: CodeGeneratorContext,
      generator: ExprCodeGenerator): GeneratedExpression = {
    val fieldExprs = distinctInfo.argIndexes.map(generateInputAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      _,
      nullableInput = false,
      deepCopy = inputFieldCopy))

    // the key expression of MapView
    if (fieldExprs.length > 1) {
      val keyTerm = newName(DISTINCT_KEY_TERM)
      val outRowWriter = newName(DEFAULT_OUT_RECORD_WRITER_TERM)
      val valueType = RowType.of(
        fieldExprs.map(_.resultType): _*)

      // always create a new result row
      generator.generateResultExpression(
        fieldExprs,
        valueType,
        classOf[BinaryRow],
        outRow = keyTerm,
        outRowWriter = Some(outRowWriter),
        reusedOutRow = false)
    } else {
      val fieldExpr = fieldExprs.head
      val keyTerm = newName(DISTINCT_KEY_TERM)
      val bType = boxedTypeTermForType(fieldExpr.resultType)
      val code =
        s"""
           |${fieldExpr.code}
           |$bType $keyTerm = ($bType) ${fieldExpr.resultTerm};
           |if (${fieldExpr.nullTerm}) {
           |  $keyTerm = null;
           |}
         """.stripMargin
      GeneratedExpression(keyTerm, fieldExpr.nullTerm, code, fieldExpr.resultType)
    }
  }

  /**
    * This method is mainly the same as CodeGenUtils.generateFieldAccess(), the only difference is
    * that this method using UpdatableRow to wrap BaseRow to handle DataViews.
    */
  private def generateAccumulatorAccess(
    ctx: CodeGeneratorContext,
    inputType: LogicalType,
    inputTerm: String,
    index: Int,
    useStateDataView: Boolean,
    useBackupDataView: Boolean,
    nullableInput: Boolean = false): GeneratedExpression = {

    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>

        val expr = if (distinctInfo.dataViewSpec.nonEmpty && useStateDataView) {
          val spec = distinctInfo.dataViewSpec.get
          val dataViewTerm = if (useBackupDataView) {
            createDataViewBackupTerm(spec)
          } else {
            createDataViewTerm(spec)
          }
          val resultTerm = if (useBackupDataView) {
            distinctBackupAccTerm
          } else {
            distinctAccTerm
          }

          val code = if (hasNamespace) {
            val expr = generateFieldAccess(ctx, inputType, inputTerm, index)
            s"""
               |// when namespace is null, the dataview is used in heap, no key and namespace set
               |if ($NAMESPACE_TERM != null) {
               |  $dataViewTerm.setCurrentNamespace($NAMESPACE_TERM);
               |  $resultTerm = $dataViewTerm;
               |} else {
               |  ${expr.code}
               |  $resultTerm = ($MAP_VIEW) ${expr.resultTerm}.getJavaObject();
               |}
            """.stripMargin
          } else {
            s"""
               |$resultTerm = $dataViewTerm;
            """.stripMargin
          }
          GeneratedExpression(resultTerm, NEVER_NULL, code, internalAccType)
        } else {
          val expr = generateFieldAccess(ctx, inputType, inputTerm, index)
          if (useBackupDataView) {
            // this is called in the merge method
            val otherMapViewTerm = newName("otherMapView")
            val code =
              s"""
                 |${expr.code}
                 |$MAP_VIEW $otherMapViewTerm = null;
                 |if (!${expr.nullTerm}) {
                 | $otherMapViewTerm = ${genToExternal(ctx, externalAccType, expr.resultTerm)};
                 |}
               """.stripMargin
            GeneratedExpression(otherMapViewTerm, expr.nullTerm, code, internalAccType)
          } else {
            val code =
              s"""
                 |${expr.code}
                 |$distinctAccTerm = ($MAP_VIEW) ${expr.resultTerm}.getJavaObject();
              """.stripMargin
            GeneratedExpression(distinctAccTerm, NEVER_NULL, code, internalAccType)
          }
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, expr)
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  // ---------------------------- Distinct Value Code Generator ---------------------------

  /**
    * The [[DistinctValueGenerator]] is an abstraction for generating codes about the
    * distinct value.
    *
    * The value of distinct state maybe long or long[] depends on the input stream
    * and the number of the distinct aggregates.
    *
    * 1. when the input is not a retraction stream, and the distinct agg number <= 64,
    *   then long is used as the value, each bit indicate whether each condition is satisfied.
    *
    * 2. when the input is not a retraction stream, and the distinct agg number > 64,
    *   then long[] is used as the value, each bit indicate whether each condition is satisfied.
    *
    * 3. when the input is a retraction stream, and the distinct agg number == 1,
    *   then long is used as the value, the long indicates the number of elements
    *   satisfy the aggregate condition.
    *
    * 4. when the input is a retraction stream, and the distinct agg number > 1,
    *   then long[] is used as the value, each long indicate the number of elements
    * *   satisfy each aggregate condition.
    */
  trait DistinctValueGenerator {
    /** the type of value of distinct state */
    def valueTypeTerm: String

    /** the default value of value of distinct state */
    def initialValue: String

    /** Accumulate the value of distinct state,
      * and generates each accumulate codes for every aggregates. */
    def foreachAccumulate(
      valueTerm: String,
      innerAccumulateCodes: Array[String],
      filterResults: Array[Option[String]]): String

    /**
      * Retract the value of distinct state,
      * and generates each retract codes for every aggregates. */
    def foreachRetract(
      valueTerm: String,
      innerRetractCodes: Array[String],
      filterResults: Array[Option[String]]): String

    /** Merge the value of distinct state,
      * and generates accumulate/retract codes when needed. */
    def foreachMerge(
      thisValueTerm: String,
      otherValueTerm: String,
      innerAccumulateCodes: Array[String],
      innerRetractCodes: Array[String]): String
  }

  /** Create a [[DistinctValueGenerator]] instance for current [[DistinctAggCodeGen]] */
  private def createDistinctValueGenerator(): DistinctValueGenerator = {
    if (!consumeRetraction) {
      if (aggCount <= JLong.SIZE) {
        new LongValueWithoutRetractionGenerator
      } else {
        new LongArrayValueWithoutRetractionGenerator
      }
    } else {
      if (aggCount <= 1) {
        new LongValueWithRetractionGenerator
      } else {
        new LongArrayValueWithRetractionGenerator
      }
    }
  }

  /** The generator used in non-retraction stream and number of aggregate <= 64 */
  class LongValueWithoutRetractionGenerator extends DistinctValueGenerator {
    checkArgument(aggCount <= JLong.SIZE)

    override def valueTypeTerm: String = "java.lang.Long"

    override def initialValue: String = "0L"

    override def foreachAccumulate(
        valueTerm: String,
        innerAccumulateCodes: Array[String],
        filterResults: Array[Option[String]]): String = {

      val codes = for (index <- filterResults.indices) yield {
        val existedTerm = newName("existed")
        val code =
          s"""
             |long $existedTerm = ((long) $valueTerm) & (1L << $index);
             |if ($existedTerm == 0) {  // not existed
             |  $valueTerm = ((long) $valueTerm) | (1L << $index);
             |  $isValueChangedTerm = true;
             |  ${innerAccumulateCodes(index)}
             |}
           """.stripMargin
        filterResults(index) match {
          case None => code
          case Some(f) =>
            s"""
               |if ($f) {
               |  $code
               |}
             """.stripMargin
        }
      }

      codes.mkString("\n")
    }

    override def foreachRetract(
        valueTerm: String,
        innerRetractCodes: Array[String],
        filterResults: Array[Option[String]]): String = {
      throw new TableException("LongValueAppendGenerator do not support retract, " +
                                 "this method should never be called, please file a issue.")
    }

    override def foreachMerge(
        thisValueTerm: String,
        otherValueTerm: String,
        innerAccumulateCodes: Array[String],
        innerRetractCodes: Array[String] /* retract code is not used here */ ): String = {

      val codes = for (index <- innerAccumulateCodes.indices) yield {
        val existedTerm = newName("existed")
        s"""
           |long $existedTerm = ((long) $thisValueTerm) & (1L << $index);
           |if ($existedTerm == 0) {  // not existed
           |  long otherExisted = ((long) $otherValueTerm) & (1L << $index);
           |  if (otherExisted != 0) {  // existed in other
           |     $isValueChangedTerm = true;
           |     // do accumulate
           |     ${innerAccumulateCodes(index)}
           |  }
           |}
         """.stripMargin
      }

      s"""
         |${codes.mkString("\n")}
         |$thisValueTerm = ((long) $thisValueTerm) | ((long) $otherValueTerm);
         |$isValueEmptyTerm = false;
       """.stripMargin
    }
  }

  /** The generator used in non-retraction stream and number of aggregate > 64 */
  class LongArrayValueWithoutRetractionGenerator extends DistinctValueGenerator {
    checkArgument(aggCount > JLong.SIZE)

    override def valueTypeTerm: String = "long[]"

    override def initialValue: String = s"new long[${aggCount / JLong.SIZE + 1}]"

    override def foreachAccumulate(
        valueTerm: String,
        innerAccumulateCodes: Array[String],
        filterResults: Array[Option[String]]): String = {

      val codes = for (index <- filterResults.indices) yield {
        val existedTerm = newName("existed")
        val arrayIndex = index / JLong.SIZE
        val bitIndex = index % JLong.SIZE
        val code =
          s"""
             |long $existedTerm = $valueTerm[$arrayIndex] & (1L << $bitIndex);
             |if ($existedTerm == 0) {  // not existed
             |  $isValueChangedTerm = true;
             |  $valueTerm[$arrayIndex] = $valueTerm[$arrayIndex] | (1L << $bitIndex);
             |  ${innerAccumulateCodes(index)}
             |}
           """.stripMargin
        filterResults(index) match {
          case None => code
          case Some(f) =>
            s"""
               |if ($f) {
               |  $code
               |}
             """.stripMargin
        }
      }

      codes.mkString("\n")
    }

    override def foreachRetract(
        valueTerm: String,
        innerRetractCodes: Array[String],
        filterResults: Array[Option[String]]): String = {
      throw new TableException("LongArrayValueAppendGenerator do not support retract, " +
                                 "this method should never be called, please file a issue.")
    }

    override def foreachMerge(
        thisValueTerm: String,
        otherValueTerm: String,
        innerAccumulateCodes: Array[String],
        innerRetractCodes: Array[String]): String = {
      val codes = for (index <- innerAccumulateCodes.indices) yield {
        val existedTerm = newName("thisExisted")
        val arrayIndex = index / JLong.SIZE
        val bitIndex = index % JLong.SIZE
        s"""
           |long $existedTerm = $thisValueTerm[$arrayIndex] & (1L << $bitIndex);
           |if ($existedTerm == 0) {  // not existed
           |  long otherExisted = $otherValueTerm[$arrayIndex] & (1L << $bitIndex);
           |  if (otherExisted != 0) {  // existed in other
           |     $isValueChangedTerm = true;
           |     // do accumulate
           |     ${innerAccumulateCodes(index)}
           |  }
           |}
         """.stripMargin
      }

      val setValueCodes = for (index <- 0 until (aggCount / JLong.SIZE + 1)) yield {
        s"$thisValueTerm[$index] |= $otherValueTerm[$index];"
      }

      s"""
         |${codes.mkString("\n")}
         |${setValueCodes.mkString("\n")}
         |$isValueEmptyTerm = false;
       """.stripMargin
    }
  }

  /** The generator used in retraction stream and only one aggregate */
  class LongValueWithRetractionGenerator extends DistinctValueGenerator {
    checkArgument(aggCount == 1)

    override def valueTypeTerm: String = "java.lang.Long"

    override def initialValue: String = "0L"

    override def foreachAccumulate(
        countTerm: String,
        innerAccumulateCodes: Array[String],
        filterResults: Array[Option[String]]): String = {
      foreachAction(isAccumulate = true, countTerm, innerAccumulateCodes, filterResults)
    }

    override def foreachRetract(
        countTerm: String,
        innerRetractCodes: Array[String],
        filterResults: Array[Option[String]]): String = {
      foreachAction(isAccumulate = false, countTerm, innerRetractCodes, filterResults)
    }

    private def foreachAction(
        isAccumulate: Boolean,
        countTerm: String,
        innerCodes: Array[String],
        filterResults: Array[Option[String]]): String = {

      val code = if (isAccumulate) {
        s"""
           |$countTerm += 1;
           |if ($countTerm == 1) {  // cnt is 0 before
           |  ${innerCodes.mkString("\n")}
           |}
           """.stripMargin
      } else {
        s"""
           |$countTerm -= 1;
           |if ($countTerm == 0) {  // cnt is +1 before
           |  ${innerCodes.mkString("\n")}
           |}
           """.stripMargin
      }

      filterResults.head match {
        case None =>
          s"""
             |$code
             |$isValueEmptyTerm = $countTerm == 0L;
           """.stripMargin
        case Some(f) =>
          s"""
             |if ($f) {
             |  $code
             |}
             |$isValueEmptyTerm = $countTerm == 0L;
             """.stripMargin
      }
    }

    override def foreachMerge(
        thisCountTerm: String,
        otherCountTerm: String,
        innerAccumulateCodes: Array[String],
        innerRetractCodes: Array[String]): String = {

      val mergedCntTerm = newName("mergedCnt")
      s"""
         |long $mergedCntTerm = $thisCountTerm + $otherCountTerm;
         |if ($mergedCntTerm == 0) {
         |  $isValueEmptyTerm = true;
         |  if ($thisCountTerm > 0) {
         |    // origin is > 0, and retract to 0, do retract
         |    ${innerRetractCodes.mkString("\n")}
         |  }
         |} else if ($mergedCntTerm < 0) {
         |  if ($thisCountTerm > 0) {
         |    // origin is > 0, and retract to < 0, do retract
         |    ${innerRetractCodes.mkString("\n")}
         |  }
         |} else if ($mergedCntTerm > 0) {
         |  if ($thisCountTerm <= 0) {
         |    // origin is <= 0, and accumulate to > 0, do accumulate
         |    ${innerAccumulateCodes.mkString("\n")}
         |  }
         |}
         |$thisCountTerm = $mergedCntTerm;
         |$isValueChangedTerm = true;
       """.stripMargin
    }
  }

  /** The generator used in retraction stream and number of aggregate > 1 */
  class LongArrayValueWithRetractionGenerator extends DistinctValueGenerator {
    checkArgument(aggCount > 1)

    override def valueTypeTerm: String = "long[]"

    override def initialValue: String = s"new long[$aggCount]"

    override def foreachAccumulate(
      valueTerm: String,
      innerAccumulateCodes: Array[String],
      filterResults: Array[Option[String]]): String = {
      foreachAction(isAccumulate = true, valueTerm, innerAccumulateCodes, filterResults)
    }

    override def foreachRetract(
      valueTerm: String,
      innerRetractCodes: Array[String],
      filterResults: Array[Option[String]]): String = {
      foreachAction(isAccumulate = false, valueTerm, innerRetractCodes, filterResults)
    }

    private def foreachAction(
      isAccumulate: Boolean,
      valueTerm: String,
      innerCodes: Array[String],
      filterResults: Array[Option[String]]): String = {

      val codes = for (index <- filterResults.indices) yield {
        val countTerm = newName("count")
        val code = if (isAccumulate) {
          s"""
             |long $countTerm = $valueTerm[$index] + 1;
             |$valueTerm[$index] = $countTerm;
             |if ($countTerm == 1) {  // cnt is 0 before
             |  ${innerCodes(index)}
             |}
           """.stripMargin
        } else {
          s"""
             |long $countTerm = $valueTerm[$index] - 1;
             |$valueTerm[$index] = $countTerm;
             |if ($countTerm == 0) {  // cnt is +1 before
             |  ${innerCodes(index)}
             |}
           """.stripMargin
        }
        filterResults(index) match {
          case None => code
          case Some(f) =>
            s"""
               |if ($f) {
               |  $code
               |}
             """.stripMargin
        }
      }

      val isEmptyCode =
        s"""
           |for (long cnt : $valueTerm) {
           |  if (cnt != 0) {
           |    $isValueEmptyTerm = false;
           |    break;
           |  }
           |}
         """.stripMargin

      s"""
         |${codes.mkString("\n")}
         |$isEmptyCode
       """.stripMargin
    }

    override def foreachMerge(
        thisValueTerm: String,
        otherValueTerm: String,
        innerAccumulateCodes: Array[String],
        innerRetractCodes: Array[String]): String = {

      val codes = for (index <- innerAccumulateCodes.indices) yield {
        val thisCountTerm = newName("thisCnt")
        val mergedCntTerm = newName("mergedCnt")
        s"""
           |long $thisCountTerm = $thisValueTerm[$index];
           |long $mergedCntTerm = $thisCountTerm + $otherValueTerm[$index];
           |if ($mergedCntTerm == 0) {
           |  if ($thisCountTerm > 0) {
           |    // origin is > 0, and retract to 0, do retract
           |    ${innerRetractCodes(index)}
           |  }
           |} else if ($mergedCntTerm < 0) {
           |  if ($thisCountTerm > 0) {
           |    // origin is > 0, and retract to < 0, do retract
           |    ${innerRetractCodes(index)}
           |  }
           |} else if ($mergedCntTerm > 0) {
           |  if ($thisCountTerm <= 0) {
           |    // origin is <= 0, and accumulate to > 0, do accumulate
           |    ${innerAccumulateCodes(index)}
           |  }
           |}
           |$thisValueTerm[$index] = $mergedCntTerm;
       """.stripMargin
      }

      val isEmptyCode =
        s"""
           |for (long cnt : $thisValueTerm) {
           |  if (cnt != 0) {
           |    $isValueEmptyTerm = false;
           |    break;
           |  }
           |}
           |$isValueChangedTerm = true;
         """.stripMargin

      s"""
         |${codes.mkString("\n")}
         |$isEmptyCode
       """.stripMargin
    }
  }
}
