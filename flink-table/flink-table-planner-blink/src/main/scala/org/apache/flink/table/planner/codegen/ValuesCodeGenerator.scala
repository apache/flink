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

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.runtime.operators.values.ValuesInputFormat
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex.RexLiteral

import java.util

import scala.collection.JavaConversions._

object ValuesCodeGenerator {

  def generatorInputFormat(
    config: TableConfig,
    outputType: RowType,
    tuples: util.List[util.List[RexLiteral]],
    description: String): ValuesInputFormat = {
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, false)
    // generate code for every record
    val generatedRecords = tuples.map { r =>
      exprGenerator.generateResultExpression(
        r.map(exprGenerator.generateExpression), outputType, classOf[GenericRowData])
    }

    // generate input format
    val generatedFunction = InputFormatCodeGenerator.generateValuesInputFormat[RowData](
      ctx,
      description,
      generatedRecords.map(_.code),
      outputType)

    new ValuesInputFormat(generatedFunction, InternalTypeInfo.of(outputType))
  }

}
