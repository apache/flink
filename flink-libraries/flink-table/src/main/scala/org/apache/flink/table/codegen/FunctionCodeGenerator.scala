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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForTypeInfo, newName}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.util.Collector

/**
  * A code generator for generating Flink [[org.apache.flink.api.common.functions.Function]]s.
  * Including [[MapFunction]], [[FlatMapFunction]], [[FlatJoinFunction]], [[ProcessFunction]].
  *
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input1 type information about the first input of the Function
  * @param input2 type information about the second input if the Function is binary
  * @param input1FieldMapping additional mapping information for input1
  *   (e.g. POJO types have no deterministic field order and some input fields might not be read)
  * @param input2FieldMapping additional mapping information for input2
  *   (e.g. POJO types have no deterministic field order and some input fields might not be read)
  */
class FunctionCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input1: TypeInformation[_ <: Any],
    input2: Option[TypeInformation[_ <: Any]] = None,
    input1FieldMapping: Option[Array[Int]] = None,
    input2FieldMapping: Option[Array[Int]] = None)
  extends CodeGenerator(
    config,
    nullableInput,
    input1,
    input2,
    input1FieldMapping,
    input2FieldMapping) {

  /**
    * A code generator for generating unary Flink
    * [[org.apache.flink.api.common.functions.Function]]s with one input.
    *
    * @param config configuration that determines runtime behavior
    * @param nullableInput input(s) can be null.
    * @param input type information about the input of the Function
    * @param inputFieldMapping additional mapping information necessary for input
    *   (e.g. POJO types have no deterministic field order and some input fields might not be read)
    */
  def this(
    config: TableConfig,
    nullableInput: Boolean,
    input: TypeInformation[Any],
    inputFieldMapping: Array[Int]) =
    this(config, nullableInput, input, None, Some(inputFieldMapping))

  /**
    * Generates a [[org.apache.flink.api.common.functions.Function]] that can be passed to Java
    * compiler.
    *
    * @param name Class name of the Function. Must not be unique but has to be a valid Java class
    *             identifier.
    * @param clazz Flink Function to be generated.
    * @param bodyCode code contents of the SAM (Single Abstract Method). Inputs, collector, or
    *                 output record can be accessed via the given term methods.
    * @param returnType expected return type
    * @tparam F Flink Function to be generated.
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  def generateFunction[F <: Function, T <: Any](
      name: String,
      clazz: Class[F],
      bodyCode: String,
      returnType: TypeInformation[T])
    : GeneratedFunction[F, T] = {
    val funcName = newName(name)
    val collectorTypeTerm = classOf[Collector[Any]].getCanonicalName

    // Janino does not support generics, that's why we need
    // manual casting here
    val (functionClass, signature, inputStatements) =
    // FlatMapFunction
    if (clazz == classOf[FlatMapFunction[_, _]]) {
      val baseClass = classOf[RichFlatMapFunction[_, _]]
      val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
      (baseClass,
        s"void flatMap(Object _in1, $collectorTypeTerm $collectorTerm)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
    }

    // MapFunction
    else if (clazz == classOf[MapFunction[_, _]]) {
      val baseClass = classOf[RichMapFunction[_, _]]
      val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
      (baseClass,
        "Object map(Object _in1)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
    }

    // FlatJoinFunction
    else if (clazz == classOf[FlatJoinFunction[_, _, _]]) {
      val baseClass = classOf[RichFlatJoinFunction[_, _, _]]
      val inputTypeTerm1 = boxedTypeTermForTypeInfo(input1)
      val inputTypeTerm2 = boxedTypeTermForTypeInfo(input2.getOrElse(
        throw new CodeGenException("Input 2 for FlatJoinFunction should not be null")))
      (baseClass,
        s"void join(Object _in1, Object _in2, $collectorTypeTerm $collectorTerm)",
        List(s"$inputTypeTerm1 $input1Term = ($inputTypeTerm1) _in1;",
             s"$inputTypeTerm2 $input2Term = ($inputTypeTerm2) _in2;"))
    }

    // JoinFunction
    else if (clazz == classOf[JoinFunction[_, _, _]]) {
      val baseClass = classOf[RichJoinFunction[_, _, _]]
      val inputTypeTerm1 = boxedTypeTermForTypeInfo(input1)
      val inputTypeTerm2 = boxedTypeTermForTypeInfo(input2.getOrElse(
        throw new CodeGenException("Input 2 for JoinFunction should not be null")))
      (baseClass,
        s"Object join(Object _in1, Object _in2)",
        List(s"$inputTypeTerm1 $input1Term = ($inputTypeTerm1) _in1;",
          s"$inputTypeTerm2 $input2Term = ($inputTypeTerm2) _in2;"))
    }

    // ProcessFunction
    else if (clazz == classOf[ProcessFunction[_, _]]) {
      val baseClass = classOf[ProcessFunction[_, _]]
      val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
      val contextTypeTerm = classOf[ProcessFunction[Any, Any]#Context].getCanonicalName

      // make context accessible also for split code
      val globalContext = if (hasCodeSplits) {
        // declaration
        reusableMemberStatements.add(s"private $contextTypeTerm $contextTerm;")
        // assignment
        List(s"this.$contextTerm = $contextTerm;")
      } else {
        Nil
      }

      (baseClass,
        s"void processElement(Object _in1, $contextTypeTerm $contextTerm, " +
          s"$collectorTypeTerm $collectorTerm)",
        List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;") ++ globalContext)
    }
    else {
      // TODO more functions
      throw new CodeGenException("Unsupported Function.")
    }

    val funcCode = j"""
      |public class $funcName extends ${functionClass.getCanonicalName} {
      |
      |  ${reuseMemberCode()}
      |
      |  public $funcName() throws Exception {
      |    ${reuseInitCode()}
      |  }
      |
      |  ${reuseConstructorCode(funcName)}
      |
      |  @Override
      |  public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
      |    ${reuseOpenCode()}
      |  }
      |
      |  @Override
      |  public $signature throws Exception {
      |    ${inputStatements.mkString("\n")}
      |    ${reuseInputUnboxingCode()}
      |    ${reusePerRecordCode()}
      |    $bodyCode
      |  }
      |
      |  @Override
      |  public void close() throws Exception {
      |    ${reuseCloseCode()}
      |  }
      |}
      |""".stripMargin

    GeneratedFunction(funcName, returnType, funcCode)
  }
}
