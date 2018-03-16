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

package org.apache.flink.table.plan.schema

import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.sql.`type`.BasicSqlType

/**
  * Creates a time indicator type for event-time or processing-time, but with similar properties
  * as a basic SQL type.
  */
class TimeIndicatorRelDataType(
    typeSystem: RelDataTypeSystem,
    originalType: BasicSqlType,
    val isEventTime: Boolean)
  extends BasicSqlType(
    typeSystem,
    originalType.getSqlTypeName,
    originalType.getPrecision) {

  override def equals(other: Any): Boolean = other match {
    case that: TimeIndicatorRelDataType =>
      super.equals(that) &&
        isEventTime == that.isEventTime
    case _ => false
  }

  override def hashCode(): Int = {
    super.hashCode() + 42 // we change the hash code to differentiate from regular timestamps
  }

  override def toString: String = {
    s"TIME ATTRIBUTE(${if (isEventTime) "ROWTIME" else "PROCTIME"})"
  }
}
