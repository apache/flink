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

import org.apache.flink.table.dataformat.{BaseRow, GenericRow, UpdatableRow}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.UserDefinedAggregateFunction
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.generateFieldAccess
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.dataview.DataViewSpec
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.toRexInputRef
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.{getAggFunctionUDIMethod, getAggUserDefinedInputTypes, getUserDefinedMethod, internalTypesToClasses, signatureToString}
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.planner.utils.SingleElementIterator
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter.getInternalClassForType
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.{ClassDataTypeConverter, PlannerTypeUtils}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.util.Collector

import org.apache.calcite.tools.RelBuilder

import java.lang.reflect.ParameterizedType
import java.lang.{Iterable => JIterable}

import scala.collection.mutable.ArrayBuffer

/**
  * It is for code generate aggregation functions that are specified in terms of
  * accumulate(), retract() and merge() functions. The aggregate accumulator is
  * embedded inside of a larger shared aggregation buffer.
  *
  * @param ctx the code gen context
  * @param aggInfo  the aggregate information
  * @param filterExpression filter argument access expression, none if no filter
  * @param mergedAccOffset the mergedAcc may come from local aggregate,
  *                        this is the first buffer offset in the row
  * @param aggBufferOffset  the offset in the buffers of this aggregate
  * @param aggBufferSize  the total size of aggregate buffers
  * @param inputTypes   the input field type infos
  * @param constantExprs  the constant expressions
  * @param relBuilder  the rel builder to translate expressions to calcite rex nodes
  * @param hasNamespace  whether the accumulators state has namespace
  * @param inputFieldCopy    copy input field element if true (only mutable type will be copied)
  */
class ImperativeAggCodeGen(
    ctx: CodeGeneratorContext,
    aggInfo: AggregateInfo,
    filterExpression: Option[Expression],
    mergedAccOffset: Int,
    aggBufferOffset: Int,
    aggBufferSize: Int,
    inputTypes: Seq[LogicalType],
    constantExprs: Seq[GeneratedExpression],
    relBuilder: RelBuilder,
    hasNamespace: Boolean,
    mergedAccOnHeap: Boolean,
    mergedAccExternalType: DataType,
    inputFieldCopy: Boolean)
  extends AggCodeGen {

  private val SINGLE_ITERABLE = className[SingleElementIterator[_]]
  private val UPDATABLE_ROW = className[UpdatableRow]

  val function = aggInfo.function.asInstanceOf[UserDefinedAggregateFunction[_, _]]
  val functionTerm: String = ctx.addReusableFunction(
    function,
    contextTerm = s"$STORE_TERM.getRuntimeContext()")
  val aggIndex: Int = aggInfo.aggIndex

  val externalAccType = aggInfo.externalAccTypes(0)
  private val internalAccType = fromDataTypeToLogicalType(externalAccType)

  /** whether the acc type is an internal type.
    * Currently we only support GenericRow as internal acc type */
  val isAccTypeInternal: Boolean =
    classOf[BaseRow].isAssignableFrom(externalAccType.getConversionClass)

  val accInternalTerm: String = s"agg${aggIndex}_acc_internal"
  val accExternalTerm: String = s"agg${aggIndex}_acc_external"
  val accTypeInternalTerm: String = if (isAccTypeInternal) {
    GENERIC_ROW
  } else {
    boxedTypeTermForType(fromDataTypeToLogicalType(externalAccType))
  }
  val accTypeExternalTerm: String = typeTerm(externalAccType.getConversionClass)

  val argTypes: Array[LogicalType] = {
    val types = inputTypes ++ constantExprs.map(_.resultType)
    aggInfo.argIndexes.map(types(_))
  }

  private val externalResultType = aggInfo.externalResultType
  private val internalResultType = fromDataTypeToLogicalType(externalResultType)

  private val rexNodeGen = new ExpressionConverter(relBuilder)

  val viewSpecs: Array[DataViewSpec] = aggInfo.viewSpecs
  // add reusable dataviews to context
  addReusableStateDataViews(ctx, viewSpecs, hasNamespace, !mergedAccOnHeap)

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    // do not set dataview into the acc in createAccumulator
    val accField = if (isAccTypeInternal) {
      // do not need convert to internal type
      s"($accTypeInternalTerm) $functionTerm.createAccumulator()"
    } else {
      genToInternal(ctx, externalAccType, s"$functionTerm.createAccumulator()")
    }
    val accInternal = newName("acc_internal")
    val code = s"$accTypeInternalTerm $accInternal = ($accTypeInternalTerm) $accField;"
    Seq(GeneratedExpression(accInternal, "false", code, internalAccType))
  }

  def setAccumulator(generator: ExprCodeGenerator): String = {
    // generate internal acc field
    val expr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      aggBufferOffset,
      viewSpecs,
      useStateDataView = true,
      useBackupDataView = false)

    if (isAccTypeInternal) {
      ctx.addReusableMember(s"private $accTypeInternalTerm $accInternalTerm;")
      s"$accInternalTerm = ${expr.resultTerm};"
    } else {
      ctx.addReusableMember(s"private $accTypeInternalTerm $accInternalTerm;")
      ctx.addReusableMember(s"private $accTypeExternalTerm $accExternalTerm;")
      s"""
         |$accInternalTerm = ${expr.resultTerm};
         |$accExternalTerm = ${genToExternal(ctx, externalAccType, accInternalTerm)};
      """.stripMargin
    }
  }

  override def resetAccumulator(generator: ExprCodeGenerator): String = {
    if (isAccTypeInternal) {
      s"$accInternalTerm = ($accTypeInternalTerm) $functionTerm.createAccumulator();"
    } else {
      s"""
         |$accExternalTerm = ($accTypeExternalTerm) $functionTerm.createAccumulator();
         |$accInternalTerm = ${genToInternal(ctx, externalAccType, accExternalTerm)};
       """.stripMargin
    }
  }

  def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    val code = if (isAccTypeInternal) {
      // do not need convert to internal type
      ""
    } else {
      s"$accInternalTerm = ${genToInternal(ctx, externalAccType, accExternalTerm)};"
    }
    Seq(GeneratedExpression(accInternalTerm, "false", code, internalAccType))
  }

  def accumulate(generator: ExprCodeGenerator): String = {
    val (parameters, code) = aggParametersCode(generator)
    // TODO handle accumulate has primitive parameters
    val call = s"$functionTerm.accumulate($parameters);"
    filterExpression match {
      case None =>
        s"""
           |$code
           |$call
         """.stripMargin
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  $code
           |  $call
           |}
         """.stripMargin
    }
  }

  def retract(generator: ExprCodeGenerator): String = {
    val (parameters, code) = aggParametersCode(generator)
    val call = s"$functionTerm.retract($parameters);"
    filterExpression match {
      case None =>
        s"""
           |$code
           |$call
         """.stripMargin
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  $code
           |  $call
           |}
         """.stripMargin
    }
  }

  def merge(generator: ExprCodeGenerator): String = {
    val accIterTerm = s"agg${aggIndex}_acc_iter"
    ctx.addReusableMember(s"private final $SINGLE_ITERABLE $accIterTerm = new $SINGLE_ITERABLE();")

    // generate internal acc field
    val expr = generateAccumulatorAccess(
      ctx,
      generator.input1Type,
      generator.input1Term,
      mergedAccOffset + aggBufferOffset,
      viewSpecs,
      useStateDataView = !mergedAccOnHeap,
      useBackupDataView = true)

    if (isAccTypeInternal) {
      s"""
         |$accIterTerm.set(${expr.resultTerm});
         |$functionTerm.merge($accInternalTerm, $accIterTerm);
       """.stripMargin
    } else {
      val otherAccExternal = newName("other_acc_external")
      s"""
         |$accTypeExternalTerm $otherAccExternal = ${
            genToExternal(ctx, mergedAccExternalType, expr.resultTerm)};
         |$accIterTerm.set($otherAccExternal);
         |$functionTerm.merge($accExternalTerm, $accIterTerm);
      """.stripMargin
    }
  }

  def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    val valueExternalTerm = newName("value_external")
    val valueExternalTypeTerm = typeTerm(externalResultType.getConversionClass)
    val valueInternalTerm = newName("value_internal")
    val valueInternalTypeTerm = boxedTypeTermForType(internalResultType)
    val nullTerm = newName("valueIsNull")
    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    val code =
      s"""
         |$valueExternalTypeTerm $valueExternalTerm = ($valueExternalTypeTerm)
         |  $functionTerm.getValue($accTerm);
         |$valueInternalTypeTerm $valueInternalTerm =
         |  ${genToInternal(ctx, externalResultType, valueExternalTerm)};
         |boolean $nullTerm = $valueInternalTerm == null;
      """.stripMargin

    GeneratedExpression(valueInternalTerm, nullTerm, code, internalResultType)
  }

  private def aggParametersCode(generator: ExprCodeGenerator): (String, String) = {
    val externalInputTypes = getAggUserDefinedInputTypes(
      function,
      externalAccType,
      argTypes)
    var codes: ArrayBuffer[String] = ArrayBuffer.empty[String]
    val inputFields = aggInfo.argIndexes.zipWithIndex.map { case (f, index) =>
      if (f >= inputTypes.length) {
        // index to constant
        val expr = constantExprs(f - inputTypes.length)
        genToExternalIfNeeded(ctx, externalInputTypes(index), expr)
      } else {
        // index to input field
        val inputRef = if (generator.input1Term.startsWith(DISTINCT_KEY_TERM)) {
          if (argTypes.length == 1) {
            // called from distinct merge and the inputTerm is the only argument
            DeclarativeExpressionResolver.toRexDistinctKey(
              relBuilder, generator.input1Term, inputTypes(f))
          } else {
            // called from distinct merge call and the inputTerm is BaseRow type
            toRexInputRef(relBuilder, index, inputTypes(f))
          }
        } else {
          // called from accumulate
          toRexInputRef(relBuilder, f, inputTypes(f))
        }
        var inputExpr = generator.generateExpression(inputRef.accept(rexNodeGen))
        if (inputFieldCopy) inputExpr = inputExpr.deepCopy(ctx)
        codes += inputExpr.code
        genToExternalIfNeeded(ctx, externalInputTypes(index), inputExpr)
      }
    }

    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    // insert acc to the head of the list
    val fields = Seq(accTerm) ++ inputFields
    // acc, arg1, arg2
    (fields.mkString(", "), codes.mkString("\n"))
  }

  /**
    * This method is mainly the same as CodeGenUtils.generateFieldAccess(), the only difference is
    * that this method using UpdatableRow to wrap BaseRow to handle DataViews.
    */
  def generateAccumulatorAccess(
    ctx: CodeGeneratorContext,
    inputType: LogicalType,
    inputTerm: String,
    index: Int,
    viewSpecs: Array[DataViewSpec],
    useStateDataView: Boolean,
    useBackupDataView: Boolean): GeneratedExpression = {

    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = ctx.getReusableInputUnboxingExprs(inputTerm, index) match {
      // input access and unboxing has already been generated
      case Some(expr) => expr

      // generate input access and unboxing if necessary
      case None =>
        // this field access is not need to reuse
        val expr = generateFieldAccess(ctx, inputType, inputTerm, index)

        val newExpr = inputType match {
          case ct: RowType if isAccTypeInternal =>
            // acc is never be null
            val fieldType = ct.getTypeAt(index).asInstanceOf[RowType]
            val exprGenerator = new ExprCodeGenerator(ctx, false)
              .bindInput(fieldType, inputTerm = expr.resultTerm)
            val converted = exprGenerator.generateConverterResultExpression(
              fieldType,
              classOf[GenericRow],
              outRecordTerm = newName("acc"),
              reusedOutRow = false,
              fieldCopy = inputFieldCopy)
            val code =
              s"""
                 |${expr.code}
                 |${ctx.reuseInputUnboxingCode(expr.resultTerm)}
                 |${converted.code}
               """.stripMargin

            GeneratedExpression(
              converted.resultTerm,
              converted.nullTerm,
              code,
              converted.resultType)
          case _ => expr
        }

        val exprWithDataView = inputType match {
          case ct: RowType if viewSpecs.nonEmpty && useStateDataView =>
            if (isAccTypeInternal) {
              val code =
                s"""
                  |${newExpr.code}
                  |${generateDataViewFieldSetter(newExpr.resultTerm, viewSpecs, useBackupDataView)}
                """.stripMargin
              GeneratedExpression(newExpr.resultTerm, newExpr.nullTerm, code, newExpr.resultType)
            } else {
              val fieldType = ct.getTypeAt(index)
              val fieldTerm = newName("field")
              ctx.addReusableMember(s"$UPDATABLE_ROW $fieldTerm;")
              val code =
                s"""
                   |${newExpr.code}
                   |$fieldTerm = null;
                   |if (!${newExpr.nullTerm}) {
                   |  $fieldTerm = new $UPDATABLE_ROW(${newExpr.resultTerm}, ${
                        PlannerTypeUtils.getArity(fieldType)});
                   |  ${generateDataViewFieldSetter(fieldTerm, viewSpecs, useBackupDataView)}
                   |}
                """.stripMargin
              GeneratedExpression(fieldTerm, newExpr.nullTerm, code, newExpr.resultType)
            }

          case _ => newExpr
        }

        ctx.addReusableInputUnboxingExprs(inputTerm, index, exprWithDataView)
        exprWithDataView
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  /**
    * Generate statements to set data view field when use state backend.
    *
    * @param accTerm aggregation term
    * @return data view field set statements
    */
  private def generateDataViewFieldSetter(
      accTerm: String,
      viewSpecs: Array[DataViewSpec],
      useBackupDataView: Boolean): String = {
    val setters = for (spec <- viewSpecs) yield {
      if (hasNamespace) {
        val dataViewTerm = if (useBackupDataView) {
          createDataViewBackupTerm(spec)
        } else {
          createDataViewTerm(spec)
        }

        val dataViewInternalTerm = if (useBackupDataView) {
          createDataViewBackupBinaryGenericTerm(spec)
        } else {
          createDataViewBinaryGenericTerm(spec)
        }

        s"""
           |// when namespace is null, the dataview is used in heap, no key and namespace set
           |if ($NAMESPACE_TERM != null) {
           |  $dataViewTerm.setCurrentNamespace($NAMESPACE_TERM);
           |  $dataViewInternalTerm.setJavaObject($dataViewTerm);
           |  $accTerm.setField(${spec.fieldIndex}, $dataViewInternalTerm);
           |}
         """.stripMargin
      } else {
        val dataViewTerm = createDataViewTerm(spec)
        val dataViewInternalTerm = createDataViewBinaryGenericTerm(spec)

        s"""
           |$dataViewInternalTerm.setJavaObject($dataViewTerm);
           |$accTerm.setField(${spec.fieldIndex}, $dataViewInternalTerm);
        """.stripMargin
      }
    }
    setters.mkString("\n")
  }

  override def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false,
      needEmitValue: Boolean = false): Unit = {

    val methodSignatures = internalTypesToClasses(argTypes)

    if (needAccumulate) {
      getAggFunctionUDIMethod(function, "accumulate", externalAccType, argTypes)
        .getOrElse(
          throw new CodeGenException(
            s"No matching accumulate method found for AggregateFunction " +
              s"'${function.getClass.getCanonicalName}'" +
              s"with parameters '${signatureToString(methodSignatures)}'.")
        )
    }

    if (needRetract) {
      getAggFunctionUDIMethod(function, "retract", externalAccType, argTypes)
        .getOrElse(
          throw new CodeGenException(
            s"No matching retract method found for AggregateFunction " +
              s"'${function.getClass.getCanonicalName}'" +
              s"with parameters '${signatureToString(methodSignatures)}'.")
        )
    }

    if (needMerge) {
      val iterType = ClassDataTypeConverter.fromClassToDataType(classOf[JIterable[Any]])
      val methods =
        getUserDefinedMethod(function, "merge", Array(externalAccType, iterType))
          .getOrElse(
            throw new CodeGenException(
              s"No matching merge method found for AggregateFunction " +
                s"${function.getClass.getCanonicalName}'.")
          )

      var iterableTypeClass = methods.getGenericParameterTypes.apply(1)
        .asInstanceOf[ParameterizedType].getActualTypeArguments.apply(0)
      // further extract iterableTypeClass if the accumulator has generic type
      iterableTypeClass match {
        case impl: ParameterizedType => iterableTypeClass = impl.getRawType
        case _ =>
      }

      val clazz = externalAccType.getConversionClass
      if (iterableTypeClass != externalAccType.getConversionClass &&
          // iterableTypeClass can be GenericRow, so classOf[BaseRow] is assignable from it.
          !getInternalClassForType(internalAccType).isAssignableFrom(
            iterableTypeClass.asInstanceOf[Class[_]])) {
        throw new CodeGenException(
          s"merge method in AggregateFunction ${function.getClass.getCanonicalName} does not " +
            s"have the correct Iterable type. Actually: $iterableTypeClass. " +
            s"Expected: $clazz")
      }
    }

    if (needReset) {
      getUserDefinedMethod(function, "resetAccumulator", Array(externalAccType))
        .getOrElse(
          throw new CodeGenException(
            s"No matching resetAccumulator method found for " +
              s"aggregate ${function.getClass.getCanonicalName}'.")
        )
    }

    if (needEmitValue) {
      val collectorDataType = ClassDataTypeConverter.fromClassToDataType(classOf[Collector[_]])
      getUserDefinedMethod(function, "emitValue", Array(externalAccType, collectorDataType))
        .getOrElse(
          throw new CodeGenException(
            s"No matching emitValue method found for " +
              s"table aggregate ${function.getClass.getCanonicalName}'.")
        )
    }
  }

  def emitValue: String = {
    val accTerm = if (isAccTypeInternal) accInternalTerm else accExternalTerm
    s"$functionTerm.emitValue($accTerm, $MEMBER_COLLECTOR_TERM);"
  }
}
