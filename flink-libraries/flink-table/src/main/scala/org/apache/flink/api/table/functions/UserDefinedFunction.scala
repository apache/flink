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

package org.apache.flink.api.table.functions

import org.apache.calcite.sql.SqlFunction
import org.apache.flink.api.table.FlinkTypeFactory
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils.checkForInstantiation

import scala.collection.mutable

/**
  * Base class for all user-defined functions such as scalar functions, table functions,
  * or aggregation functions.
  *
  * User-defined functions must have a default constructor and must be instantiable during runtime.
  */
abstract class UserDefinedFunction {

  // we cache SQL functions to reduce amount of created objects
  // (i.e. for type inference, validation, etc.)
  private val cachedSqlFunctions = mutable.HashMap[String, SqlFunction]()

  // check if function can be instantiated
  checkForInstantiation(this.getClass)

  /**
    * Returns the corresponding [[SqlFunction]]. Creates an instance if not already created.
    */
  private[flink] final def getSqlFunction(
      name: String,
      typeFactory: FlinkTypeFactory)
    : SqlFunction = {
    cachedSqlFunctions.getOrElseUpdate(name, createSqlFunction(name, typeFactory))
  }

  /**
    * Creates corresponding [[SqlFunction]].
    */
  private[flink] def createSqlFunction(
      name: String,
      typeFactory: FlinkTypeFactory)
    : SqlFunction

  override def toString = getClass.getCanonicalName
}
