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
  * A [[TableSource]] extending this interface is able to filter the fields of the return table.
  *
  */
trait FilterableTableSource {

  /** return an predicate expression that was set. */
  def getPredicate: Array[Expression]

  /**
    * @param predicate a filter expression that will be applied to fields to return.
    * @return an unsupported predicate expression.
    */
  def setPredicate(predicate: Array[Expression]): Array[Expression]
}
