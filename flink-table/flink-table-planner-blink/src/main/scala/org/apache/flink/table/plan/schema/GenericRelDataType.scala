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
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.`type`.GenericType

/**
  * Generic type for encapsulating Flink's [[TypeInformation]].
  *
  * @param genericType InternalType to encapsulate
  * @param nullable flag if type can be nullable
  * @param typeSystem Flink's type system
  */
class GenericRelDataType(
    val genericType: GenericType[_],
    val nullable: Boolean,
    typeSystem: RelDataTypeSystem)
  extends BasicSqlType(
    typeSystem,
    SqlTypeName.ANY) {

  isNullable = nullable

  override def toString = s"ANY($genericType)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[GenericRelDataType]

  override def equals(other: Any): Boolean = other match {
    case that: GenericRelDataType =>
      super.equals(that) &&
        (that canEqual this) &&
        genericType == that.genericType &&
        nullable == that.nullable
    case _ => false
  }

  override def hashCode(): Int = {
    genericType.hashCode()
  }
}
