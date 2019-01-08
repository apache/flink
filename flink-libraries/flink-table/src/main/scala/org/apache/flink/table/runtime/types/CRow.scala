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

package org.apache.flink.table.runtime.types

import org.apache.flink.types.Row

/**
  * Wrapper for a [[Row]] to add retraction information.
  *
  * If [[change]] is true, the [[CRow]] is an accumulate message, if it is false it is a
  * retraction message.
  *
  * @param row The wrapped [[Row]].
  * @param change true for an accumulate message, false for a retraction message.
  */
class CRow(var row: Row, var change: Boolean) {

  def this() {
    this(null, true)
  }

  override def toString: String = s"${if(change) "+" else "-"}$row"

  override def equals(other: scala.Any): Boolean = {
    val otherCRow = other.asInstanceOf[CRow]
    row.equals(otherCRow.row) && change == otherCRow.change
  }
}

object CRow {

  def apply(): CRow = {
    new CRow()
  }

  def apply(row: Row, change: Boolean): CRow = {
    new CRow(row, change)
  }
}
