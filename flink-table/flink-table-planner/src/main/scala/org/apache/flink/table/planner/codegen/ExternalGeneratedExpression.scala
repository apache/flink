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

import org.apache.flink.table.types.DataType

/**
 * Describes a external generated expression.
 *
 * @param dataType type of the resultTerm
 * @param internalTerm term to access the internal result of the expression
 * @param externalTerm term to access the external result of the expression
 * @param nullTerm boolean term that indicates if expression is null
 * @param internalCode code necessary to produce internalTerm and nullTerm
 * @param externalCode code necessary to produce externalTerm
 * @param literalValue None if the expression is not literal. Otherwise it represent the
 *                     original object of the literal.
 *
 */
class ExternalGeneratedExpression(
  dataType: DataType,
  internalTerm: String,
  externalTerm: String,
  nullTerm: String,
  internalCode: String,
  externalCode: String,
  literalValue: Option[Any] = None)
  extends GeneratedExpression(
    internalTerm,
    nullTerm,
    internalCode,
    dataType.getLogicalType,
    literalValue) {

  def getExternalCode: String = externalCode

  def getExternalTerm: String = externalTerm

  def getDataType: DataType = dataType

}

object ExternalGeneratedExpression {

  def fromGeneratedExpression(
    dataType: DataType,
    externalTerm: String,
    externalCode: String,
    generatedExpression: GeneratedExpression)
  : ExternalGeneratedExpression = {
    new ExternalGeneratedExpression(
      dataType,
      generatedExpression.resultTerm,
      externalTerm,
      generatedExpression.nullTerm,
      generatedExpression.code, externalCode,
      generatedExpression.literalValue)
  }
}
