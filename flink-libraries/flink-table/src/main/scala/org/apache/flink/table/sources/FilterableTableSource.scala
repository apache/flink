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

import org.apache.flink.table.expressions.Expression

/**
  * Adds support for filtering push-down to a [[TableSource]].
  * A [[TableSource]] extending this interface is able to filter records before returning.
  */
trait FilterableTableSource {

  /**
    * Indicates whether the filter push down has been applied. Note that even if we don't
    * actually push down any filters, we should also set this flag to true after the trying.
    */
  private var filterPushedDown: Boolean = false

  /**
    * Check and pick all predicates this table source can support. The passed in predicates
    * have been translated in conjunctive form, and table source and only pick those predicates
    * that it supports. All unsupported predicates should be give back to the framework to do
    * further filtering.
    *
    * @param predicate An array contains conjunctive predicates.
    * @return An array contains all unsupported predicates.
    */
  def applyPredicate(predicate: Array[Expression]): Array[Expression]

  /**
    * Return the flag to indicate whether filter push down has been tried.
    */
  def isFilterPushedDown: Boolean = filterPushedDown

  /**
    * Set the flag to indicate whether the filter push down has been tried.
    */
  def setFilterPushedDown(flag: Boolean): Unit = filterPushedDown = flag

}
