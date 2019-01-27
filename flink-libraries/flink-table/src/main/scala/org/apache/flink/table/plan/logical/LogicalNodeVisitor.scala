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

package org.apache.flink.table.plan.logical

trait LogicalNodeVisitor[T] {
  def visit(logical: LogicalNode): T
  def visit(project: Project): T
  def visit(alias: AliasNode): T
  def visit(filter: Filter): T
  def visit(aggregate: Aggregate): T
  def visit(union: Union): T
  def visit(join: Join): T
  def visit(catalog: CatalogNode): T
  def visit(windowAggregate: WindowAggregate): T
  def visit(logicalTableFunctionCall: LogicalTableFunctionCall): T
  def visit(sinkNode: SinkNode): T
}
