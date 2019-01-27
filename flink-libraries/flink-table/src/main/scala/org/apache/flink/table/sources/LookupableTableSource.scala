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
package org.apache.flink.table.sources

import org.apache.flink.table.api.functions.{AsyncTableFunction, TableFunction}


/**
  * A [[TableSource]] which supports for lookup accessing via key column(s). For example, MySQL
  * TableSource can implement this interface, then temporal join this MySQL table can be a
  * lookup fashion.
  *
  * @tparam T type of the result
  */
trait LookupableTableSource[T] extends TableSource {

  /**
    * Gets the [[TableFunction]] which supports lookup one key at a time.
    */
  def getLookupFunction(lookupKeys: Array[Int]): TableFunction[T]

  /**
    * Gets the [[AsyncTableFunction]] which supports asyc lookup one key at a time.
    */
  def getAsyncLookupFunction(lookupKeys: Array[Int]): AsyncTableFunction[T]

  /**
    * Defines the lookup behavior in the config. Such as whether to use async lookup.
    */
  def getLookupConfig: LookupConfig

}
