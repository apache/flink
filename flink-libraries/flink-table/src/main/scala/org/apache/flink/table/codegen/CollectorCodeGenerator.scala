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
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForTypeInfo, newName}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.runtime.TableFunctionCollector


/**
  * A code generator for generating [[org.apache.flink.util.Collector]]s.
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
class CollectorCodeGenerator(
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
    * Generates a [[TableFunctionCollector]] that can be passed to Java compiler.
    *
    * @param name Class name of the table function collector. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the collector method
    * @param collectedType The type information of the element collected by the collector
    * @return instance of GeneratedCollector
    */
  def generateTableFunctionCollector(
      name: String,
      bodyCode: String,
      collectedType: TypeInformation[Any])
    : GeneratedCollector = {

    val className = newName(name)
    val input1TypeClass = boxedTypeTermForTypeInfo(input1)
    val input2TypeClass = boxedTypeTermForTypeInfo(collectedType)

    // declaration in case of code splits
    val recordMember = if (hasCodeSplits) {
      s"private $input2TypeClass $input2Term;"
    } else {
      ""
    }

    // assignment in case of code splits
    val recordAssignment = if (hasCodeSplits) {
      s"$input2Term" // use member
    } else {
      s"$input2TypeClass $input2Term" // local variable
    }

    val funcCode = j"""
      |public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {
      |
      |  $recordMember
      |  ${reuseMemberCode()}
      |
      |  public $className() throws Exception {
      |    ${reuseInitCode()}
      |  }
      |
      |  @Override
      |  public void collect(Object record) throws Exception {
      |    super.collect(record);
      |    $input1TypeClass $input1Term = ($input1TypeClass) getInput();
      |    $recordAssignment = ($input2TypeClass) record;
      |    ${reuseInputUnboxingCode()}
      |    ${reusePerRecordCode()}
      |    $bodyCode
      |  }
      |
      |  @Override
      |  public void close() {
      |  }
      |}
      |""".stripMargin

    GeneratedCollector(className, funcCode)
  }

}
