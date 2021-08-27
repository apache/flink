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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}

import org.apache.calcite.plan.Context

/**
  * A [[Context]] to allow the store data within the planner session and access it within rules.
  */
trait FlinkContext extends Context {

  /**
   * Returns whether the planner runs in batch mode.
   */
  def isBatchMode: Boolean

  /**
    * Gets [[TableConfig]] instance defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getTableConfig: TableConfig

  /**
    * Gets [[FunctionCatalog]] instance defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getFunctionCatalog: FunctionCatalog

  /**
    * Gets [[CatalogManager]] instance defined in [[org.apache.flink.table.api.TableEnvironment]].
    */
  def getCatalogManager: CatalogManager

  /**
    * Gets [[SqlExprToRexConverterFactory]] instance to convert sql expression to rex node.
    */
  def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory

  override def unwrap[C](clazz: Class[C]): C = {
    if (clazz.isInstance(this)) clazz.cast(this) else null.asInstanceOf[C]
  }

}
