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

package org.apache.flink.table.validate

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.{FlinkTypeFactory, OnDemandSqlOperatorTable}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{createAggregateSqlFunction, createScalarSqlFunction, createTableSqlFunction}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.calcite.sql._
import org.apache.flink.table.catalog.{CalciteCatalogFunction, CatalogFunction, CatalogManager, ObjectPath}
import org.apache.flink.table.util.Logging

import _root_.scala.collection.JavaConversions._

/**
  * A catalog for looking up (user-defined) functions, used during validation phases
  * of both Table API and SQL API.
  * TODO Table API.
  */
class FunctionCatalog(catalogManager: CatalogManager) extends Logging {

  private def registerFunction(
      name: String,
      catalogFunction: CatalogFunction): Unit = {
    val functionPath = new ObjectPath(catalogManager.getCurrentDatabase, name)
    val catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get()

    catalog.createFunction(functionPath, catalogFunction, false)
  }

  def registerScalarFunction(
      name: String,
      function: ScalarFunction,
      typeFactory: FlinkTypeFactory): Unit = {
    registerFunction(
      name,
      new CalciteCatalogFunction(
        new ScalarFunctionDefinition(name, function),
        createScalarSqlFunction(name, name, function, typeFactory)
      )
    )
  }

  def registerTableFunction(
      name: String,
      function: TableFunction[_],
      implicitResultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory): Unit = {
    registerFunction(
      name,
      new CalciteCatalogFunction(
        new TableFunctionDefinition(name, function, implicitResultType),
        createTableSqlFunction(name, name, function, implicitResultType, typeFactory)
      )
    )
  }

  def registerAggregateFunction(
      name: String,
      function: AggregateFunction[_, _],
      resultType: TypeInformation[_],
      accType: TypeInformation[_],
      typeFactory: FlinkTypeFactory): Unit = {
    registerFunction(
      name,
      new CalciteCatalogFunction(
        new AggregateFunctionDefinition(name, function, resultType, accType),
        createAggregateSqlFunction(
          name,
          name,
          function,
          resultType,
          accType,
          typeFactory)
      )
    )
  }

  def getUserDefinedFunctions: Seq[String] = {
    catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get()
        .listFunctions(catalogManager.getCurrentDatabase)
  }

  def getSqlOperatorTable: SqlOperatorTable =
    new OnDemandSqlOperatorTable(catalogManager)
}
