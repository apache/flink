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

package org.apache.flink.table.runtime

import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{RowType, DataTypes}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}

import org.junit.Assert._
import org.junit.Test

import java.util.Date

class SubstituteStreamOperatorTest {

  private val operatorName = "SimpleStreamOperator"
  val config = new TableConfig()
  private val ctx = CodeGeneratorContext(config)

  @Test
  def testCodegenOneInputStreamOperator(): Unit = {
    val processCode = "in1.setInt(0, 2);"
    val generatedOperator = OperatorCodeGenerator.generateOneInputStreamOperator[Date, Date](
      ctx, operatorName, processCode, "", new RowType(DataTypes.DATE),
      new TableConfig)
    val substituteOperator = new OneInputSubstituteStreamOperator[Date, Date](
      generatedOperator.name, generatedOperator.code)
    val operator = substituteOperator.getActualStreamOperator(getClass.getClassLoader)
      .asInstanceOf[OneInputStreamOperator[BaseRow, BaseRow]]
    val row = GenericRow.of(new Integer(50))
    operator.processElement(new StreamRecord(row))
    assertEquals(2, row.getInt(0))
  }
}
