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

import org.apache.calcite.sql._
import org.apache.flink.table.expressions._

/**
  * A base catalog for looking up (user-defined) functions, used during validation phases
  * of both Table API and SQL API.
  */
trait FunctionCatalog {

  /**
    * Register function(through Table API) to this function catalog.
    * Different implementations may have different behaviors when registering repeatedly.
    *
    * @param name The function name.
    * @param builder The class name of the function.
    */
  def registerFunction(name: String, builder: Class[_]): Unit

  /**
    * Register function(through SQL API) to this function catalog.
    * Different implementations may have different behaviors when registering repeatedly.
    *
    * @param sqlFunction The SQL function that needs to be registered.
    */
  def registerSqlFunction(sqlFunction: SqlFunction): Unit

  /**
    * Get [[SqlOperatorTable]] from this catalog.
    *
    * @return The related SqlOperatorTable.
    */
  def getSqlOperatorTable: SqlOperatorTable

  /**
    * Lookup and create an expression if we find a match.
    */
  def lookupFunction(name: String, children: Seq[Expression]): Expression

  /**
    * Drop a function and return true if the function existed.
    */
  def dropFunction(name: String): Boolean

  /**
    * Drop all registered functions.
    */
  def clear(): Unit
}
