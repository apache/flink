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
package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.api.common.functions.{AbstractRichFunction, RichFunction}
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.table.api.{DataTypes, TableException}
import org.apache.flink.table.api.Expressions.callSql
import org.apache.flink.table.data.{GenericRowData, RawValueData, StringData}
import org.apache.flink.table.data.binary.{BinaryRawValueData, BinaryStringData}
import org.apache.flink.table.expressions.ApiExpressionUtils.{typeLiteral, unresolvedCall, unresolvedRef}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions._
import org.apache.flink.table.functions.SpecializedFunction.{ExpressionEvaluator, ExpressionEvaluatorFactory}
import org.apache.flink.table.functions.UserDefinedFunctionHelper.{validateClassForRuntime, ASYNC_TABLE_EVAL, SCALAR_EVAL, TABLE_EVAL}
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RexFactory}
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.collector.WrappingCollector
import org.apache.flink.table.runtime.functions.DefaultExpressionEvaluator
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.join.lookup.DelegatingResultFuture
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.ExtractionUtils.primitiveToWrapper
import org.apache.flink.table.types.inference.{CallContext, TypeInference, TypeInferenceUtil}
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot, RowType}
import org.apache.flink.table.types.logical.RowType.RowField
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType
import org.apache.flink.table.types.utils.DataTypeUtils.{isInternal, validateInputDataType, validateOutputDataType}
import org.apache.flink.util.Preconditions

import java.util.concurrent.CompletableFuture

import scala.collection.JavaConverters._

/**
 * Helps in generating a call to a user-defined [[ScalarFunction]], [[TableFunction]], or
 * [[AsyncTableFunction]].
 *
 * Table functions are a special case because they are using a collector. Thus, the result of the
 * generation will be a reference to a [[WrappingCollector]]. Furthermore, atomic types are wrapped
 * into a row by the collector.
 *
 * Async table functions don't support atomic types.
 */
object BridgingFunctionGenUtil {

  def generateFunctionAwareCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      inference: TypeInference,
      callContext: CallContext,
      udf: UserDefinedFunction,
      functionName: String,
      skipIfArgsNull: Boolean): GeneratedExpression = {

    val (call, _) = generateFunctionAwareCallWithDataType(
      ctx,
      operands,
      returnType,
      inference,
      callContext,
      udf,
      functionName,
      skipIfArgsNull)

    call
  }

  def generateFunctionAwareCallWithDataType(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      inference: TypeInference,
      callContext: CallContext,
      udf: UserDefinedFunction,
      functionName: String,
      skipIfArgsNull: Boolean): (GeneratedExpression, DataType) = {

    // enrich argument types with conversion class
    val adaptedCallContext = TypeInferenceUtil.adaptArguments(inference, callContext, null)
    val enrichedArgumentDataTypes = toScala(adaptedCallContext.getArgumentDataTypes)
    verifyArgumentTypes(operands.map(_.resultType), enrichedArgumentDataTypes)

    // enrich output types with conversion class
    val enrichedOutputDataType =
      TypeInferenceUtil.inferOutputType(adaptedCallContext, inference.getOutputTypeStrategy)
    verifyFunctionAwareOutputType(returnType, enrichedOutputDataType, udf)

    // find runtime method and generate call
    verifyFunctionAwareImplementation(
      enrichedArgumentDataTypes,
      enrichedOutputDataType,
      udf,
      functionName)
    val call = generateFunctionAwareCall(
      ctx,
      operands,
      enrichedArgumentDataTypes,
      enrichedOutputDataType,
      returnType,
      udf,
      skipIfArgsNull)
    (call, enrichedOutputDataType)
  }

  private def generateFunctionAwareCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType,
      returnType: LogicalType,
      udf: UserDefinedFunction,
      skipIfArgsNull: Boolean): GeneratedExpression = {

    val functionTerm = ctx.addReusableFunction(udf)

    // operand conversion
    val externalOperands = prepareExternalOperands(ctx, operands, argumentDataTypes)

    if (udf.getKind == FunctionKind.TABLE) {
      generateTableFunctionCall(
        ctx,
        functionTerm,
        externalOperands,
        outputDataType,
        returnType,
        skipIfArgsNull)
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE) {
      generateAsyncTableFunctionCall(functionTerm, externalOperands, returnType)
    } else {
      generateScalarFunctionCall(ctx, functionTerm, externalOperands, outputDataType)
    }
  }

  private def generateTableFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      functionOutputDataType: DataType,
      outputType: LogicalType,
      skipIfArgsNull: Boolean): GeneratedExpression = {
    val resultCollectorTerm = generateResultCollector(ctx, functionOutputDataType, outputType)

    val setCollectorCode = s"""
                              |$functionTerm.setCollector($resultCollectorTerm);
                              |""".stripMargin
    ctx.addReusableOpenStatement(setCollectorCode)

    val functionCallCode = if (skipIfArgsNull) {
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |if (${externalOperands.map(_.nullTerm).mkString(" || ")}) {
         |  // skip
         |} else {
         |  $functionTerm.eval(${externalOperands.map(_.resultTerm).mkString(", ")});
         |}
         |""".stripMargin
    } else {
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |$functionTerm.eval(${externalOperands.map(_.resultTerm).mkString(", ")});
         |""".stripMargin
    }

    // has no result
    GeneratedExpression(resultCollectorTerm, NEVER_NULL, functionCallCode, outputType)
  }

  private def generateAsyncTableFunctionCall(
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      outputType: LogicalType): GeneratedExpression = {

    val DELEGATE = className[DelegatingResultFuture[_]]

    val functionCallCode =
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |if (${externalOperands.map(_.nullTerm).mkString(" || ")}) {
         |  $DEFAULT_COLLECTOR_TERM.complete(java.util.Collections.emptyList());
         |} else {
         |  $DELEGATE delegates = new $DELEGATE($DEFAULT_COLLECTOR_TERM);
         |  $functionTerm.eval(
         |    delegates.getCompletableFuture(),
         |    ${externalOperands.map(_.resultTerm).mkString(", ")});
         |}
         |""".stripMargin

    // has no result
    GeneratedExpression(NO_CODE, NEVER_NULL, functionCallCode, outputType)
  }

  /**
   * Generates a collector that converts the output of a table function (possibly as an atomic type)
   * into an internal row type. Returns a collector term for referencing the collector.
   */
  private def generateResultCollector(
      ctx: CodeGeneratorContext,
      outputDataType: DataType,
      returnType: LogicalType): String = {
    val outputType = outputDataType.getLogicalType

    val collectorCtx = new CodeGeneratorContext(ctx.tableConfig, ctx.classLoader)
    val externalResultTerm = newName("externalResult")

    // code for wrapping atomic types
    val collectorCode = if (!isCompositeType(outputType)) {
      val resultGenerator = new ExprCodeGenerator(collectorCtx, outputType.isNullable)
        .bindInput(outputType, externalResultTerm)
      val wrappedResult = resultGenerator.generateConverterResultExpression(
        returnType.asInstanceOf[RowType],
        classOf[GenericRowData])
      s"""
         |${wrappedResult.code}
         |outputResult(${wrappedResult.resultTerm});
         |""".stripMargin
    } else {
      s"""
         |if ($externalResultTerm != null) {
         |  outputResult($externalResultTerm);
         |}
         |""".stripMargin
    }

    // collector for converting to internal types then wrapping atomic types
    val resultCollector = CollectorCodeGenerator.generateWrappingCollector(
      collectorCtx,
      "TableFunctionResultConverterCollector",
      outputType,
      externalResultTerm,
      // nullability is handled by the expression code generator if necessary
      genToInternalConverter(ctx, outputDataType),
      collectorCode
    )
    val resultCollectorTerm = newName("resultConverterCollector")
    CollectorCodeGenerator.addToContext(ctx, resultCollectorTerm, resultCollector)

    resultCollectorTerm
  }

  private def generateScalarFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      outputDataType: DataType): GeneratedExpression = {

    // result conversion
    val externalResultClass = outputDataType.getConversionClass
    val externalResultTypeTerm = typeTerm(externalResultClass)
    // Janino does not fully support the JVM spec:
    // boolean b = (boolean) f(); where f returns Object
    // This is not supported and we need to box manually.
    val externalResultClassBoxed = primitiveToWrapper(externalResultClass)
    val externalResultCasting = if (externalResultClass == externalResultClassBoxed) {
      s"($externalResultTypeTerm)"
    } else {
      s"($externalResultTypeTerm) (${typeTerm(externalResultClassBoxed)})"
    }
    val externalResultTerm = ctx.addReusableLocalVariable(externalResultTypeTerm, "externalResult")
    val externalCode =
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |$externalResultTerm = $externalResultCasting $functionTerm
         |  .$SCALAR_EVAL(${externalOperands.map(_.resultTerm).mkString(", ")});
         |""".stripMargin

    val internalExpr = genToInternalConverterAll(ctx, outputDataType, externalResultTerm)

    val copy = internalExpr.copy(code = s"""
                                           |$externalCode
                                           |${internalExpr.code}
                                           |""".stripMargin)

    ExternalGeneratedExpression.fromGeneratedExpression(
      outputDataType,
      externalResultTerm,
      externalCode,
      copy)
  }

  private def prepareExternalOperands(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType]): Seq[GeneratedExpression] = {
    operands
      .zip(argumentDataTypes)
      .map {
        case (operand, dataType) =>
          operand match {
            case external: ExternalGeneratedExpression
                if !isInternal(dataType) && (external.getDataType == dataType) =>
              operand.copy(resultTerm = external.getExternalTerm, code = external.getExternalCode)
            case _ => operand.copy(resultTerm = genToExternalConverterAll(ctx, dataType, operand))
          }
      }
  }

  private def verifyArgumentTypes(
      operandTypes: Seq[LogicalType],
      enrichedDataTypes: Seq[DataType]): Unit = {
    val enrichedTypes = enrichedDataTypes.map(_.getLogicalType)
    operandTypes.zip(enrichedTypes).foreach {
      case (operandType, enrichedType) =>
        // check that the logical type has not changed during the enrichment
        if (!supportsAvoidingCast(operandType, enrichedType)) {
          throw new CodeGenException(
            s"Mismatch of function's argument data type '$enrichedType' and actual " +
              s"argument type '$operandType'.")
        }
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    enrichedDataTypes.foreach(validateOutputDataType)
  }

  private def verifyFunctionAwareOutputType(
      returnType: LogicalType,
      enrichedDataType: DataType,
      udf: UserDefinedFunction): Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    if (udf.getKind == FunctionKind.TABLE && !isCompositeType(enrichedType)) {
      // logically table functions wrap atomic types into ROW, however, the physical function might
      // return an atomic type
      Preconditions.checkState(
        returnType.is(LogicalTypeRoot.ROW) && returnType.getChildren.size() == 1,
        "Logical output type of function call should be a ROW wrapping an atomic type.",
        Seq(): _*
      )
      val atomicOutputType = returnType.asInstanceOf[RowType].getChildren.get(0)
      verifyOutputType(atomicOutputType, enrichedDataType)
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE && !isCompositeType(enrichedType)) {
      throw new CodeGenException(
        "Async table functions must not emit an atomic type. " +
          "Only a composite type such as the row type are supported.")
    } else if (udf.getKind == FunctionKind.TABLE || udf.getKind == FunctionKind.ASYNC_TABLE) {
      // null values are skipped therefore, the result top level row will always be not null
      verifyOutputType(returnType.copy(true), enrichedDataType)
    } else {
      verifyOutputType(returnType, enrichedDataType)
    }
  }

  private def verifyOutputType(returnType: LogicalType, enrichedDataType: DataType): Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    // check that the logical type has not changed during the enrichment
    if (!supportsAvoidingCast(enrichedType, returnType)) {
      throw new CodeGenException(
        s"Mismatch of expected output data type '$returnType' and function's " +
          s"output type '$enrichedType'.")
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    validateInputDataType(enrichedDataType)
  }

  def verifyFunctionAwareImplementation(
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType,
      udf: UserDefinedFunction,
      functionName: String): Unit = {
    if (udf.getKind == FunctionKind.TABLE) {
      verifyImplementation(TABLE_EVAL, argumentDataTypes, None, udf, functionName)
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE) {
      verifyImplementation(
        ASYNC_TABLE_EVAL,
        DataTypes.NULL.bridgedTo(classOf[CompletableFuture[_]]) +: argumentDataTypes,
        None,
        udf,
        functionName)
    } else if (udf.getKind == FunctionKind.SCALAR) {
      verifyImplementation(SCALAR_EVAL, argumentDataTypes, Some(outputDataType), udf, functionName)
    } else {
      throw new CodeGenException(
        s"Unsupported function kind '${udf.getKind}' for function '$functionName'.")
    }
  }

  private def verifyImplementation(
      methodName: String,
      argumentDataTypes: Seq[DataType],
      outputDataType: Option[DataType],
      udf: UserDefinedFunction,
      functionName: String): Unit = {
    val argumentClasses = argumentDataTypes.map(_.getConversionClass).toArray
    val outputClass = outputDataType.map(_.getConversionClass).getOrElse(classOf[Unit])
    validateClassForRuntime(udf.getClass, methodName, argumentClasses, outputClass, functionName)
  }

  class DefaultExpressionEvaluatorFactory(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      rexFactory: RexFactory)
    extends ExpressionEvaluatorFactory {

    override def createEvaluator(
        function: BuiltInFunctionDefinition,
        outputDataType: DataType,
        args: DataType*): ExpressionEvaluator = {
      val (argFields, call) = function match {
        case BuiltInFunctionDefinitions.CAST | BuiltInFunctionDefinitions.TRY_CAST =>
          Preconditions.checkArgument(args.length == 1, "Casting expects one arguments.", Seq(): _*)
          val field = DataTypes.FIELD("arg0", args.head)
          (
            Seq(field),
            unresolvedCall(function, unresolvedRef(field.getName), typeLiteral(outputDataType)))
        case _ =>
          val fields = args.zipWithIndex
            .map { case (dataType, i) => DataTypes.FIELD(s"arg$i", dataType) }
          val argRefs = fields.map(arg => unresolvedRef(arg.getName))
          (fields, unresolvedCall(function, argRefs: _*))
      }

      createEvaluator(call, outputDataType, argFields: _*)
    }

    override def createEvaluator(
        sqlExpression: String,
        outputDataType: DataType,
        args: DataTypes.Field*): ExpressionEvaluator = {
      createEvaluator(callSql(sqlExpression), outputDataType, args: _*)
    }

    override def createEvaluator(
        expression: Expression,
        outputDataType: DataType,
        args: DataTypes.Field*): ExpressionEvaluator = {
      args.foreach(f => validateInputDataType(f.getDataType))
      validateOutputDataType(outputDataType)

      try {
        createEvaluatorOrError(expression, outputDataType, args)
      } catch {
        case t: Throwable =>
          throw new TableException(
            s"Unable to create an expression evaluator for expression: $expression",
            t)
      }
    }

    /**
     * This method generates code and wraps it into a [[DefaultExpressionEvaluator]].
     *
     * For example, executing the following:
     * {{{
     *   createEvaluator("a = b", BOOLEAN(), FIELD("a", DataTypes.INT()), FIELD("b", INT()))
     * }}}
     * would result in:
     * {{{
     * public class ExpressionEvaluator$20 extends org.apache.flink.api.common.functions.AbstractRichFunction {
     *
     *   public ExpressionEvaluator$20(Object[] references) throws Exception {}
     *
     *   public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {}
     *
     *   public java.lang.Boolean eval(java.lang.Integer arg0, java.lang.Integer arg1) {
     *     int result$16;
     *     boolean isNull$16;
     *     int result$17;
     *     boolean isNull$17;
     *     boolean isNull$18;
     *     boolean result$19;
     *
     *     isNull$16 = arg0 == null;
     *     result$16 = -1;
     *     if (!isNull$16) {
     *       result$16 = arg0;
     *     }
     *     isNull$17 = arg1 == null;
     *     result$17 = -1;
     *     if (!isNull$17) {
     *       result$17 = arg1;
     *     }
     *
     *     boolean $0IsNull = isNull$16;
     *     int $0 = result$16;
     *
     *     boolean $1IsNull = isNull$17;
     *     int $1 = result$17;
     *
     *     isNull$18 = $0IsNull || $1IsNull;
     *     result$19 = false;
     *     if (!isNull$18) {
     *       result$19 = $0 == $1;
     *     }
     *
     *     return (java.lang.Boolean) (isNull$18 ? null : ((java.lang.Boolean) result$19));
     *   }
     * }
     * }}}
     */
    private def createEvaluatorOrError(
        expression: Expression,
        outputDataType: DataType,
        args: Seq[DataTypes.Field]): ExpressionEvaluator = {
      val argFields = args.map(f => new RowField(f.getName, f.getDataType.getLogicalType))
      val outputType = outputDataType.getLogicalType

      val ctx = new EvaluatorCodeGeneratorContext(tableConfig, classLoader)

      val externalOutputClass = outputDataType.getConversionClass
      val externalOutputTypeTerm = typeTerm(externalOutputClass)

      // arguments
      val externalArgClasses = args
        .map(_.getDataType.getConversionClass)
        .map {
          clazz =>
            // special cases to be in sync with typeTerm(...)
            if (clazz == classOf[StringData]) {
              classOf[BinaryStringData]
            } else if (clazz == classOf[RawValueData[_]]) {
              classOf[BinaryRawValueData[_]]
            } else {
              clazz
            }
        }
      val externalArgTypeTerms = externalArgClasses.map(typeTerm)
      val argsSignatureCode = externalArgTypeTerms.zipWithIndex
        .map { case (t, i) => s"$t arg$i" }
        .mkString(", ")
      val argToInternalExprs = args
        .map(_.getDataType)
        .zipWithIndex
        .map {
          case (argDataType, i) =>
            genToInternalConverterAll(ctx, argDataType, s"arg$i")
        }

      // map arguments
      val argMappingCode = argToInternalExprs.zipWithIndex
        .map {
          case (srcExpr, i) =>
            val newResultTerm = "$" + i
            val newResultTypeTerm = primitiveTypeTermForType(srcExpr.resultType)
            val newNullTerm = newResultTerm + "IsNull"
            s"""
               |boolean $newNullTerm = ${srcExpr.nullTerm};
               |$newResultTypeTerm $newResultTerm = ${srcExpr.resultTerm};
               |""".stripMargin
        }
        .mkString("\n")

      // expression
      val rexNode = rexFactory.convertExpressionToRex(argFields.asJava, expression, outputType)
      val rexNodeType = FlinkTypeFactory.toLogicalType(rexNode.getType)
      if (!supportsAvoidingCast(rexNodeType, outputType)) {
        throw new CodeGenException(
          s"Mismatch between expression type '$rexNodeType' and expected type '$outputType'.")
      }
      val exprCodeGen = new ExprCodeGenerator(ctx, false)
      val genExpr = exprCodeGen.generateExpression(rexNode)

      // output
      val resultTerm = genToExternalConverterAll(ctx, outputDataType, genExpr)
      val externalResultClass = outputDataType.getConversionClass
      val externalResultTypeTerm = typeTerm(externalResultClass)
      val externalResultClassBoxed = primitiveToWrapper(externalResultClass)
      val externalResultCasting = if (externalResultClass == externalResultClassBoxed) {
        s"($externalResultTypeTerm)"
      } else {
        s"($externalResultTypeTerm) (${typeTerm(externalResultClassBoxed)})"
      }

      val evaluatorName = newName("ExpressionEvaluator")
      val evaluatorCode =
        s"""
           |public class $evaluatorName extends ${className[AbstractRichFunction]} {
           |
           |  ${ctx.reuseMemberCode()}
           |
           |  public $evaluatorName(Object[] references) throws Exception {
           |    ${ctx.reuseInitCode()}
           |  }
           |
           |  public void open(${className[Configuration]} parameters) throws Exception {
           |    ${ctx.reuseOpenCode()}
           |  }
           |
           |  public $externalOutputTypeTerm eval($argsSignatureCode) {
           |    ${ctx.reuseLocalVariableCode()}
           |    ${argToInternalExprs.map(_.code).mkString("\n")}
           |    $argMappingCode
           |    ${genExpr.code}
           |    return $externalResultCasting ($resultTerm);
           |  }
           |}
           |""".stripMargin

      val genClass = new GeneratedFunction[RichFunction](
        evaluatorName,
        evaluatorCode,
        ctx.references.toArray,
        ctx.tableConfig)
      new DefaultExpressionEvaluator(
        genClass,
        externalResultClass,
        externalArgClasses.toArray,
        rexNode.toString)
    }
  }

  private class EvaluatorCodeGeneratorContext(tableConfig: ReadableConfig, classLoader: ClassLoader)
    extends CodeGeneratorContext(tableConfig, classLoader) {

    override def addReusableConverter(
        dataType: DataType,
        classLoaderTerm: String = null): String = {
      super.addReusableConverter(dataType, "this.getClass().getClassLoader()")
    }

    override def addReusableFunction(
        function: UserDefinedFunction,
        functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
        contextArgs: Seq[String] = null): String = {
      super.addReusableFunction(
        function,
        classOf[FunctionContext],
        Seq("null, this.getClass().getClassLoader()", "null"))
    }
  }
}
