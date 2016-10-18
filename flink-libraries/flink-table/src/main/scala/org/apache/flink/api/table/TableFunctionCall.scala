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
package org.apache.flink.api.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.{Expression, UnresolvedFieldReference}
import org.apache.flink.api.table.functions.TableFunction
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils.getFieldInfo
import org.apache.flink.api.table.plan.logical.{LogicalNode, LogicalTableFunctionCall}


/**
  * A [[TableFunctionCall]] represents a call to a [[TableFunction]] with actual parameters.
  *
  * For Scala users, Flink will help to parse a [[TableFunction]] to [[TableFunctionCall]]
  * implicitly. For Java users, Flink will help to parse a string expression to
  * [[TableFunctionCall]]. So users do not need to create a [[TableFunctionCall]] manually.
  *
  * @param functionName function name
  * @param tableFunction user-defined table function
  * @param parameters actual parameters of function
  * @param resultType type information of returned table
  */
case class TableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[Expression],
    resultType: TypeInformation[_]) {

  private var aliases: Option[Seq[Expression]] = None

  /**
    * Assigns an alias for this table function returned fields that the following `select()` clause
    * can refer to.
    *
    * @param aliasList alias for this table function returned fields
    * @return this table function call
    */
  def as(aliasList: Expression*): TableFunctionCall = {
    this.aliases = Some(aliasList)
    this
  }

  /**
    * Converts an API class to a logical node for planning.
    */
  private[flink] def toLogicalTableFunctionCall(child: LogicalNode): LogicalTableFunctionCall = {
    val originNames = getFieldInfo(resultType)._1

    // determine the final field names
    val fieldNames = if (aliases.isDefined) {
      val aliasList = aliases.get
      if (aliasList.length != originNames.length) {
        throw ValidationException(
          s"List of column aliases must have same degree as table; " +
            s"the returned table of function '$functionName' has ${originNames.length} " +
            s"columns (${originNames.mkString(",")}), " +
            s"whereas alias list has ${aliasList.length} columns")
      } else if (!aliasList.forall(_.isInstanceOf[UnresolvedFieldReference])) {
        throw ValidationException("Alias only accept name expressions as arguments")
      } else {
        aliasList.map(_.asInstanceOf[UnresolvedFieldReference].name).toArray
      }
    } else {
      originNames
    }

    LogicalTableFunctionCall(
      functionName,
      tableFunction,
      parameters,
      resultType,
      fieldNames,
      child)
  }
}


case class TableFunctionCallBuilder[T: TypeInformation](udtf: TableFunction[T]) {
  /**
    * Creates a call to a [[TableFunction]] in Scala Table API.
    *
    * @param params actual parameters of function
    * @return [[TableFunctionCall]]
    */
  def apply(params: Expression*): TableFunctionCall = {
    val resultType = if (udtf.getResultType == null) {
      implicitly[TypeInformation[T]]
    } else {
      udtf.getResultType
    }
    TableFunctionCall(udtf.getClass.getSimpleName, udtf, params, resultType)
  }
}

