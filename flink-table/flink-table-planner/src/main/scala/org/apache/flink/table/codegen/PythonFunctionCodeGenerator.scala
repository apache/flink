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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.codegen.CodeGenUtils.{primitiveDefaultValue, primitiveTypeTermForTypeInfo, newName}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.functions.{UserDefinedFunction, ScalarFunction}
import org.apache.flink.table.functions.python.{PythonEnv, PythonFunction}
import org.apache.flink.table.utils.EncodingUtils

/**
  * A code generator for generating Python [[UserDefinedFunction]]s.
  */
object PythonFunctionCodeGenerator extends Compiler[UserDefinedFunction] {

  private val PYTHON_SCALAR_FUNCTION_NAME = "PythonScalarFunction"

  /**
    * Generates a [[ScalarFunction]] for the specified Python user-defined function.
    *
    * @param name name of the user-defined function
    * @param serializedScalarFunction serialized Python scalar function
    * @param inputTypes input data types
    * @param resultType expected result type
    * @param deterministic the determinism of the function's results
    * @param pythonEnv the Python execution environment
    * @return instance of generated ScalarFunction
    */
  def generateScalarFunction(
      name: String,
      serializedScalarFunction: Array[Byte],
      inputTypes: Array[TypeInformation[_]],
      resultType: TypeInformation[_],
      deterministic: Boolean,
      pythonEnv: PythonEnv): ScalarFunction = {
    val funcName = newName(PYTHON_SCALAR_FUNCTION_NAME)
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultResultValue = primitiveDefaultValue(resultType)
    val inputParamCode = inputTypes.zipWithIndex.map { case (inputType, index) =>
      s"${primitiveTypeTermForTypeInfo(inputType)} in$index"
    }.mkString(", ")

    val encodingUtilsTypeTerm = classOf[EncodingUtils].getCanonicalName
    val typeInfoTypeTerm = classOf[TypeInformation[_]].getCanonicalName
    val inputTypesCode = inputTypes.map(EncodingUtils.encodeObjectToString).map { inputType =>
      s"""
         |($typeInfoTypeTerm) $encodingUtilsTypeTerm.decodeStringToObject(
         |  "$inputType", $typeInfoTypeTerm.class)
         |""".stripMargin
    }.mkString(", ")

    val encodedResultType = EncodingUtils.encodeObjectToString(resultType)
    val encodedScalarFunction = EncodingUtils.encodeBytesToBase64(serializedScalarFunction)
    val encodedPythonEnv = EncodingUtils.encodeObjectToString(pythonEnv)
    val pythonEnvTypeTerm = classOf[PythonEnv].getCanonicalName

    val funcCode = j"""
      |public class $funcName extends ${classOf[ScalarFunction].getCanonicalName}
      |  implements ${classOf[PythonFunction].getCanonicalName} {
      |
      |  private static final long serialVersionUID = 1L;
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
      |    return ($typeInfoTypeTerm) $encodingUtilsTypeTerm.decodeStringToObject(
      |      "$encodedResultType", $typeInfoTypeTerm.class);
      |  }
      |
      |  @Override
      |  public byte[] getSerializedPythonFunction() {
      |    return $encodingUtilsTypeTerm.decodeBase64ToBytes("$encodedScalarFunction");
      |  }
      |
      |  @Override
      |  public $pythonEnvTypeTerm getPythonEnv() {
      |    return ($pythonEnvTypeTerm) $encodingUtilsTypeTerm.decodeStringToObject(
      |      "$encodedPythonEnv", $pythonEnvTypeTerm.class);
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

    val clazz = compile(
      Thread.currentThread().getContextClassLoader,
      funcName,
      funcCode)
    clazz.newInstance().asInstanceOf[ScalarFunction]
  }
}
