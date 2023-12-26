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
package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, RichAsyncFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.{FilterCondition, GeneratedFilterCondition, GeneratedFunction, GeneratedJoinCondition, JoinCondition}
import org.apache.flink.table.types.logical.LogicalType

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
   * @param ctx
   *   The context of the code generator
   * @param name
   *   Class name of the Function. Must not be unique but has to be a valid Java class identifier.
   * @param clazz
   *   Flink Function to be generated.
   * @param bodyCode
   *   code contents of the SAM (Single Abstract Method). Inputs, collector, or output record can be
   *   accessed via the given term methods.
   * @param returnType
   *   expected return type
   * @param input1Type
   *   the first input type
   * @param input1Term
   *   the first input term
   * @param input2Type
   *   the second input type, optional.
   * @param input2Term
   *   the second input term.
   * @param collectorTerm
   *   the collector term
   * @param contextTerm
   *   the context term
   * @tparam F
   *   Flink Function to be generated.
   * @return
   *   instance of GeneratedFunction
   */
  def generateFunction[F <: Function](
      ctx: CodeGeneratorContext,
      name: String,
      clazz: Class[F],
      bodyCode: String,
      returnType: LogicalType,
      input1Type: LogicalType,
      input1Term: String = DEFAULT_INPUT1_TERM,
      input2Type: Option[LogicalType] = None,
      input2Term: Option[String] = Some(DEFAULT_INPUT2_TERM),
      collectorTerm: String = DEFAULT_COLLECTOR_TERM,
      contextTerm: String = DEFAULT_CONTEXT_TERM): GeneratedFunction[F] = {
    val funcName = newName(name)
    val inputTypeTerm = boxedTypeTermForType(input1Type)

    // Janino does not support generics, that's why we need
    // manual casting here
    val samHeader =
      // FlatMapFunction
      if (clazz == classOf[FlatMapFunction[_, _]]) {
        val baseClass = classOf[RichFlatMapFunction[_, _]]
        (
          baseClass,
          s"void flatMap(Object _in1, org.apache.flink.util.Collector $collectorTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      }

      // MapFunction
      else if (clazz == classOf[MapFunction[_, _]]) {
        val baseClass = classOf[RichMapFunction[_, _]]
        (
          baseClass,
          "Object map(Object _in1)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      }

      // FlatJoinFunction
      else if (clazz == classOf[FlatJoinFunction[_, _, _]]) {
        val baseClass = classOf[RichFlatJoinFunction[_, _, _]]
        val inputTypeTerm2 = boxedTypeTermForType(
          input2Type.getOrElse(
            throw new CodeGenException("Input 2 for FlatJoinFunction should not be null")))
        (
          baseClass,
          s"void join(Object _in1, Object _in2, org.apache.flink.util.Collector $collectorTerm)",
          List(
            s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;",
            s"$inputTypeTerm2 ${input2Term.get} = ($inputTypeTerm2) _in2;"))
      }

      // ProcessFunction
      else if (clazz == classOf[ProcessFunction[_, _]]) {
        val baseClass = classOf[ProcessFunction[_, _]]
        (
          baseClass,
          s"void processElement(Object _in1, " +
            s"org.apache.flink.streaming.api.functions.ProcessFunction.Context $contextTerm," +
            s"org.apache.flink.util.Collector $collectorTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      }

      // AsyncFunction
      else if (clazz == classOf[AsyncFunction[_, _]]) {
        val baseClass = classOf[RichAsyncFunction[_, _]]
        (
          baseClass,
          s"void asyncInvoke(Object _in1, " +
            s"org.apache.flink.streaming.api.functions.async.ResultFuture $collectorTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      } else {
        // TODO more functions
        throw new CodeGenException("Unsupported Function.")
      }

    val funcCode =
      j"""
      ${ctx.getClassHeaderComment}
      public class $funcName
          extends ${samHeader._1.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
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
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    new GeneratedFunction(funcName, funcCode, ctx.references.toArray, ctx.tableConfig)
  }

  /**
   * Generates a [[AbstractRichFunction]] class code that can be passed to Java compiler. Including
   * [[JoinCondition]] and [[FilterCondition]] rich functions.
   *
   * @param ctx
   *   The context of the code generator
   * @param name
   *   Class name of the Function. Not must be unique but has to be a valid Java class identifier.
   * @param clazz
   *   Function to be generated.
   * @param bodyCode
   *   code contents of the SAM (Single Abstract Method).
   * @param input1Term
   *   the first input term
   * @param input2Term
   *   the second input term.
   * @return
   *   the generated condition function name and code
   */
  private def generateCondition[F <: Function](
      ctx: CodeGeneratorContext,
      name: String,
      clazz: Class[F],
      bodyCode: String,
      input1Term: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      input2Term: String = CodeGenUtils.DEFAULT_INPUT2_TERM): (String, String) = {
    val funcName = newName(name)

    val methodHeader = {
      if (clazz == classOf[JoinCondition]) {
        s"apply($ROW_DATA $input1Term, $ROW_DATA $input2Term)"
      } else if (clazz == classOf[FilterCondition]) {
        s"apply($ROW_DATA $input1Term)"
      } else {
        throw new CodeGenException(s"Unsupported Condition Function $clazz.")
      }
    }
    val funcCode =
      j"""
      public class $funcName extends ${className[AbstractRichFunction]}
          implements ${clazz.getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        ${ctx.reuseConstructorCode(funcName)}

        @Override
        public void open(${className[Configuration]} parameters) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public boolean $methodHeader throws Exception {
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() throws Exception {
          super.close();
          ${ctx.reuseCloseCode()}
        }
      }
     """.stripMargin

    (funcName, funcCode)
  }

  /**
   * Generates a [[JoinCondition]] that can be passed to Java compiler.
   *
   * @param ctx
   *   The context of the code generator
   * @param name
   *   Class name of the Function. Not must be unique but has to be a valid Java class identifier.
   * @param bodyCode
   *   code contents of the SAM (Single Abstract Method).
   * @param input1Term
   *   the first input term
   * @param input2Term
   *   the second input term.
   * @return
   *   instance of GeneratedJoinCondition
   */
  def generateJoinCondition(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      input1Term: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      input2Term: String = CodeGenUtils.DEFAULT_INPUT2_TERM): GeneratedJoinCondition = {

    val (funcName, funcCode) =
      generateCondition(ctx, name, classOf[JoinCondition], bodyCode, input1Term, input2Term)

    new GeneratedJoinCondition(funcName, funcCode, ctx.references.toArray, ctx.tableConfig)
  }

  /**
   * Generates a [[FilterCondition]] that can be passed to Java compiler.
   *
   * @param ctx
   *   The context of the code generator
   * @param name
   *   Class name of the Function. Not must be unique but has to be a valid Java class identifier.
   * @param bodyCode
   *   code contents of the SAM (Single Abstract Method).
   * @param inputTerm
   *   the input term
   * @return
   *   instance of GeneratedFilterCondition
   */
  def generateFilterCondition(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT_TERM): GeneratedFilterCondition = {

    val (funcName, funcCode) =
      generateCondition(ctx, name, classOf[FilterCondition], bodyCode, inputTerm)

    new GeneratedFilterCondition(funcName, funcCode, ctx.references.toArray, ctx.tableConfig)
  }
}
