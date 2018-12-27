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

package org.apache.flink.table.functions.utils

import java.util

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._

import scala.collection.JavaConversions._

/**
  * Calcite wrapper for user-defined table aggregate functions.
  *
  * @param name                   function name (used by SQL parser)
  * @param displayName            name to be displayed in operator name
  * @param tableAggregateFunction table aggregate function to be called
  * @param alias                  the alias names for the output fields
  * @param returnType             the type information of returned value
  * @param accType                the type information of the accumulator
  * @param typeFactory            type factory for converting Flink's between Calcite's types
  */
class TableAggSqlFunction(
    name: String,
    displayName: String,
    tableAggregateFunction: TableAggregateFunction[_, _],
    alias: Option[Seq[String]],
    val returnType: TypeInformation[_],
    val accType: TypeInformation[_],
    typeFactory: FlinkTypeFactory)
  extends SqlUserDefinedAggFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    createReturnTypeInference(returnType, typeFactory),
    createOperandTypeInference(tableAggregateFunction, typeFactory),
    createOperandTypeChecker(tableAggregateFunction),
    // Do not need to provide a calcite aggregateFunction here. Flink aggregation function
    // will be generated when translating the calcite relnode to flink runtime execution plan
    null,
    false,
    false,
    typeFactory
  ) {

  def getRelTypeAfterAlias(flinkTypeFactory: FlinkTypeFactory): RelDataType = {
    val builder = flinkTypeFactory.builder
    val relDataType = flinkTypeFactory.createTypeFromTypeInfo(returnType, true)
    val fieldNames: Seq[String] = if (alias.isDefined) {
      alias.get
    } else {
      relDataType.getFieldNames
    }

    fieldNames
      .zip(relDataType.getFieldList)
      .foreach { f =>
        builder.add(f._1, f._2.getType)
      }
    builder.build()
  }

  def getFunction: TableAggregateFunction[_, _] = tableAggregateFunction

  override def isDeterministic: Boolean = tableAggregateFunction.isDeterministic

  override def toString: String = displayName

  override def getParamTypes: util.List[RelDataType] = null
}

object TableAggSqlFunction {

  def apply(
      name: String,
      displayName: String,
      aggregateFunction: TableAggregateFunction[_, _],
      alias: Option[Seq[String]],
      returnType: TypeInformation[_],
      accType: TypeInformation[_],
      typeFactory: FlinkTypeFactory): TableAggSqlFunction = {

    new TableAggSqlFunction(
      name,
      displayName,
      aggregateFunction,
      alias,
      returnType,
      accType,
      typeFactory)
  }
}


