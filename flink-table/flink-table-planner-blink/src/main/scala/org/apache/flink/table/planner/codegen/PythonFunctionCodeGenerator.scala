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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.python.{PythonEnv, PythonFunction}
import org.apache.flink.table.functions.{ScalarFunction, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{newName, primitiveDefaultValue, primitiveTypeTermForType}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter

/**
  * A code generator for generating Python [[UserDefinedFunction]]s.
  */
object PythonFunctionCodeGenerator {

  private val PYTHON_SCALAR_FUNCTION_NAME = "PythonScalarFunction"

  /**
    * Generates a [[ScalarFunction]] for the specified Python user-defined function.
    *
    * @param ctx The context of the code generator
    * @param name name of the user-defined function
    * @param serializedScalarFunction serialized Python scalar function
    * @param inputTypes input data types
    * @param resultType expected result type
    * @param deterministic the determinism of the function's results
    * @param pythonEnv the Python execution environment
    * @return instance of generated ScalarFunction
    */
  def generateScalarFunction(
      ctx: CodeGeneratorContext,
      name: String,
      serializedScalarFunction: Array[Byte],
      inputTypes: Array[TypeInformation[_]],
      resultType: TypeInformation[_],
      deterministic: Boolean,
      pythonEnv: PythonEnv): ScalarFunction = {
    val funcName = newName(PYTHON_SCALAR_FUNCTION_NAME)
    val resultLogicType = TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType(resultType)
    val resultTypeTerm = primitiveTypeTermForType(resultLogicType)
    val defaultResultValue = primitiveDefaultValue(resultLogicType)
    val inputParamCode = inputTypes.zipWithIndex.map { case (inputType, index) =>
      s"${primitiveTypeTermForType(
        TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType(inputType))} in$index"
    }.mkString(", ")

    val typeInfoTypeTerm = classOf[TypeInformation[_]].getCanonicalName
    val pythonEnvTypeTerm = classOf[PythonEnv].getCanonicalName

    val resultTypeNameTerm =
      ctx.addReusableObject(resultType, "resultType", typeInfoTypeTerm)
    val serializedScalarFunctionNameTerm =
      ctx.addReusableObject(serializedScalarFunction, "serializedScalarFunction", "byte[]")
    val pythonEnvNameTerm = ctx.addReusableObject(pythonEnv, "pythonEnv", pythonEnvTypeTerm)
    val inputTypesCode = inputTypes
      .map(ctx.addReusableObject(_, "inputType", typeInfoTypeTerm))
      .mkString(", ")

    val funcCode = j"""
      |public class $funcName extends ${classOf[ScalarFunction].getCanonicalName}
      |  implements ${classOf[PythonFunction].getCanonicalName} {
      |
      |  private static final long serialVersionUID = 1L;
      |
      |  ${ctx.reuseMemberCode()}
      |
      |  public $funcName(Object[] references) throws Exception {
      |     ${ctx.reuseInitCode()}
      |  }
      |
      |  public $resultTypeTerm eval($inputParamCode) {
      |    return $defaultResultValue;
      |  }
      |
      |  @Override
      |  public $typeInfoTypeTerm[] getParameterTypes(Class<?>[] signature) {
      |    return new $typeInfoTypeTerm[]{$inputTypesCode};
      |  }
      |
      |  @Override
      |  public $typeInfoTypeTerm getResultType(Class<?>[] signature) {
      |    return $resultTypeNameTerm;
      |  }
      |
      |  @Override
      |  public byte[] getSerializedPythonFunction() {
      |    return $serializedScalarFunctionNameTerm;
      |  }
      |
      |  @Override
      |  public $pythonEnvTypeTerm getPythonEnv() {
      |    return $pythonEnvNameTerm;
      |  }
      |
      |  @Override
      |  public boolean isDeterministic() {
      |    return $deterministic;
      |  }
      |
      |  @Override
      |  public String toString() {
      |    return "$name";
      |  }
      |}
      |""".stripMargin
    new GeneratedFunction(funcName, funcCode, ctx.references.toArray)
      .newInstance(Thread.currentThread().getContextClassLoader)
  }
}
