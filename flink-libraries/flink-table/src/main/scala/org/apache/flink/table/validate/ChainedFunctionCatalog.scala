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

import org.apache.calcite.sql.{SqlFunction, SqlOperatorTable}
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.flink.table.expressions._

import scala.collection.JavaConversions._

/**
  * A chained catalog for looking up (user-defined) functions through a series of
  * function catalogs(including external function catalogs and built-in function catalog),
  * used during validation phases of both Table API and SQL API.
  */
class ChainedFunctionCatalog(functionCatalogs: Seq[FunctionCatalog])
  extends FunctionCatalog {

  override def registerFunction(name: String, builder: Class[_]): Unit = {
    functionCatalogs.foreach(_.registerFunction(name, builder))
  }

  override def registerSqlFunction(sqlFunction: SqlFunction): Unit = {
    functionCatalogs.foreach(_.registerSqlFunction(sqlFunction))
  }

  override def getSqlOperatorTable: SqlOperatorTable = {
    val sqlOperatorTables = functionCatalogs.map(_.getSqlOperatorTable)
    new ChainedSqlOperatorTable(sqlOperatorTables)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    var exception: Throwable = null
    functionCatalogs.foreach(functionCatalog =>
      try {
        return functionCatalog.lookupFunction(name, children)
      } catch {
        // continue lookup function from next functionCatalog
        case t: Throwable => exception = t
      }
    )
    throw exception
  }

  override def dropFunction(name: String): Boolean = {
    functionCatalogs.find(_.dropFunction(name)).isDefined
  }

  override def clear(): Unit = functionCatalogs.foreach(_.clear)

}
