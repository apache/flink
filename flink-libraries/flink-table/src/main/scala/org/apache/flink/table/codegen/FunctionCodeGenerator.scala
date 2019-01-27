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
package org.apache.flink.table.codegen

import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, RichAsyncFunction}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.codegen.CodeGenUtils.boxedTypeTermForType
import org.apache.flink.table.codegen.CodeGenUtils.getDefineParamsByFunctionClass
import org.apache.flink.table.codegen.CodeGenUtils.getCallingParamsByFunctionClass
import org.apache.flink.table.codegen.CodeGenUtils.newName
import org.apache.flink.table.codegen.CodeGenUtils.generateSplitFunctionCalls
import org.apache.flink.table.codegen.CodeGeneratorContext.BASE_ROW
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.dataformat.BaseRow

/**
  * A code generator for generating Flink [[org.apache.flink.api.common.functions.Function]]s.
  * Including [[MapFunction]], [[FlatMapFunction]], [[FlatJoinFunction]], [[ProcessFunction]], and
  * the corresponding rich version of the functions.
  */
object FunctionCodeGenerator {

  /**
   * Generates a [[org.apache.flink.api.common.functions.Function]] that can be passed to Java
   * compiler.
   *
   * @param ctx The context of the code generator
   * @param name Class name of the Function. Must not be unique but has to be a valid Java class
   *             identifier.
   * @param clazz Flink Function to be generated.
   * @param bodyCode code contents of the SAM (Single Abstract Method). Inputs, collector, or
   *                 output record can be accessed via the given term methods.
   * @param returnType expected return type
   * @param input1Type the first input type
   * @param input1Term the first input term
   * @param input2Type the second input type, optional.
   * @param input2Term the second input term.
   * @param collectorTerm the collector term
   * @param contextTerm the context term
   * @tparam F Flink Function to be generated.
   * @tparam T Return type of the Flink Function.
   * @return instance of GeneratedFunction
   */
  def generateFunction[F <: Function, T <: Any](
      ctx: CodeGeneratorContext,
      name: String,
      clazz: Class[F],
      bodyCode: String,
      returnType: InternalType,
      input1Type: InternalType,
      tableConfig: TableConfig,
      input1Term: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      input2Type: Option[InternalType] = None,
      input2Term: Option[String] = Some(CodeGeneratorContext.DEFAULT_INPUT2_TERM),
      collectorTerm: String = CodeGeneratorContext.DEFAULT_COLLECTOR_TERM,
      contextTerm: String = CodeGeneratorContext.DEFAULT_CONTEXT_TERM,
      codeSplit: GeneratedSplittableExpression =
      GeneratedSplittableExpression.UNSPLIT_EXPRESSION,
      filterCodeSplit: GeneratedSplittableExpression =
      GeneratedSplittableExpression.UNSPLIT_EXPRESSION)
  : GeneratedFunction[F, T] = {
    val funcName = newName(name)
    val inputTypeTerm = boxedTypeTermForType(input1Type.toInternalType)

    // Janino does not support generics, that's why we need
    // manual casting here
    val samHeader =
    // FlatMapFunction
    if (clazz == classOf[FlatMapFunction[_, _]]) {
      val baseClass = classOf[RichFlatMapFunction[_, _]]
      (baseClass,
        s"void flatMap(Object _in1, org.apache.flink.util.Collector $collectorTerm)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
    }

    // MapFunction
    else if (clazz == classOf[MapFunction[_, _]]) {
      val baseClass = classOf[RichMapFunction[_, _]]
      (baseClass,
        "Object map(Object _in1)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
    }

    // FlatJoinFunction
    else if (clazz == classOf[FlatJoinFunction[_, _, _]]) {
      val baseClass = classOf[RichFlatJoinFunction[_, _, _]]
      val inputTypeTerm2 = boxedTypeTermForType(input2Type.getOrElse(throw new CodeGenException(
        "Input 2 for FlatJoinFunction should not be null")).toInternalType)
      (baseClass,
        s"void join(Object _in1, Object _in2, org.apache.flink.util.Collector $collectorTerm)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;",
             s"$inputTypeTerm2 ${input2Term.get} = ($inputTypeTerm2) _in2;"))
    }

    // ProcessFunction
    else if (clazz == classOf[ProcessFunction[_, _]]) {
      val baseClass = classOf[ProcessFunction[_, _]]
      (baseClass,
        s"void processElement(Object _in1, " +
          s"org.apache.flink.streaming.api.functions.ProcessFunction.Context $contextTerm," +
          s"org.apache.flink.util.Collector $collectorTerm)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
    }

    // AsyncFunction
    else if (clazz == classOf[AsyncFunction[_, _]]) {
      val baseClass = classOf[RichAsyncFunction[_, _]]
      (baseClass,
        s"void asyncInvoke(Object _in1, " +
          s"org.apache.flink.streaming.api.functions.async.ResultFuture $collectorTerm)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
    }

    else {
      // TODO more functions
      throw new CodeGenException("Unsupported Function.")
    }

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      tableConfig.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = getDefineParamsByFunctionClass(clazz),
      callingParams = getCallingParamsByFunctionClass(clazz)
    )
    val shallSplit = unboxingCodeSplit.isSplit || codeSplit.isSplit || filterCodeSplit.isSplit
    val funcCode = if (shallSplit) {
      j"""
      public class $funcName
          extends ${samHeader._1.getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName() throws Exception {
          ${ctx.reuseInitCode()}
        }

        ${ctx.reuseConstructorCode(funcName)}

        @Override
        public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public ${samHeader._2} throws Exception {
          ${samHeader._3.mkString("\n")}
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies).map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${samHeader._3.mkString("\n")}
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                """.stripMargin
            }
          } mkString("\n")
        }

        ${
          codeSplit.definitions.zip(codeSplit.bodies).map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${samHeader._3.mkString("\n")}
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                 """.stripMargin
            }
          }.mkString("\n")
        }

        ${
          if (filterCodeSplit.isSplit) {
              filterCodeSplit.definitions.zip(filterCodeSplit.bodies).map {
                case (define, body) => {
                  s"""
                     |$define throws Exception {
                     |  ${samHeader._3.mkString("\n")}
                     |  ${ctx.reusePerRecordCode()}
                     |  $body
                     |}
                      """.stripMargin
                }
              }.mkString("\n")
          } else {
            ""
          }
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin
    } else {
      j"""
      public class $funcName
          extends ${samHeader._1.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName() throws Exception {
          ${ctx.reuseInitCode()}
        }

        ${ctx.reuseConstructorCode(funcName)}

        @Override
        public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public ${samHeader._2} throws Exception {
          ${samHeader._3.mkString("\n")}
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin
    }

    GeneratedFunction(funcName, funcCode)
  }

  def generateJoinConditionFunction(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      config: TableConfig,
      input1Term: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      input2Term: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM)
    : GeneratedJoinConditionFunction = {
    val funcName = newName(name)

    val baseClass = classOf[JoinConditionFunction]

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$BASE_ROW $input1Term, $BASE_ROW $input2Term",
      callingParams = s"$input1Term, $input2Term"
    )

    val funcCode = if (unboxingCodeSplit.isSplit) {
      j"""
      public class $funcName extends ${baseClass.getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $funcName() throws Exception {
          ${ctx.reuseInitCode()}
        }

        ${ctx.reuseConstructorCode(funcName)}

        @Override
        public boolean apply($BASE_ROW $input1Term, $BASE_ROW $input2Term) throws Exception {
          ${ctx.reusePerRecordCode()}
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${
          unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies).map {
            case (define, body) => {
              s"""
                 |$define throws Exception {
                 |  ${ctx.reusePerRecordCode()}
                 |  $body
                 |}
                 """.stripMargin
            }
          } mkString "\n"
        }
    }
    """.stripMargin
    } else {
      j"""
      public class $funcName extends ${baseClass.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName() throws Exception {
          ${ctx.reuseInitCode()}
        }

        ${ctx.reuseConstructorCode(funcName)}

        @Override
        public boolean apply($BASE_ROW $input1Term, $BASE_ROW $input2Term) throws Exception {
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }
    }
    """.stripMargin
    }

    GeneratedJoinConditionFunction(funcName, funcCode)
  }
}

/**
  * Condition Function for [[org.apache.calcite.rel.core.Join]].
  */
abstract class JoinConditionFunction {
  @throws[Exception]
  def apply(in1: BaseRow, in2: BaseRow): Boolean
}
