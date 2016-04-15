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

package org.apache.flink.api.table.plan.schema

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Schema.TableType
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{StreamableTable, Table, TranslatableTable}

/**
  * A [[org.apache.calcite.schema.Table]] implementation for registering
  * Streaming Table API Tables in the Calcite schema to be used by Flink SQL.
  * It implements [[TranslatableTable]] so that its logical scan
  * can be converted to a relational expression and [[StreamableTable]]
  * so that it can be used in Streaming SQL queries.
  *
  * Except for registering Streaming Tables, this implementation is also used
  * in [[org.apache.flink.api.table.plan.rules.LogicalScanToStreamable]]
  * rule to convert a logical scan of a non-Streamable Table into
  * a logical scan of a Streamable table, i.e. of this class.
  *
  * @see [[DataStreamTable]]
  */
class TransStreamTable(relNode: RelNode, wrapper: Boolean)
  extends AbstractTable
  with TranslatableTable
  with StreamableTable {

  override def getJdbcTableType: TableType = ???

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = relNode.getRowType

  override def stream(): Table = {
    if (wrapper) {
      // we need to return a wrapper non-streamable table,
      // otherwise Calcite's rule-matching produces an infinite loop
      new StreamTable(relNode)
    }
    else {
      this
    }
  }

  override def toRel(context: ToRelContext, relOptTable: RelOptTable): RelNode =
    relNode

  /**
    * Wraps a [[TransStreamTable]]'s relNode
    * to implement its stream() method.
    */
  class StreamTable(relNode: RelNode) extends AbstractTable {

    override def getJdbcTableType: TableType = ???

    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
      relNode.getRowType
    }
  }
}
