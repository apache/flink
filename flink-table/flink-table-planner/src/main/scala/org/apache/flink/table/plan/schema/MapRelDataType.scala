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

import com.google.common.base.Objects
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.MapSqlType
import org.apache.flink.api.common.typeinfo.TypeInformation

class MapRelDataType(
   val typeInfo: TypeInformation[_],
   val keyType: RelDataType,
   val valueType: RelDataType,
   isNullable: Boolean) extends MapSqlType(keyType, valueType, isNullable) {

  override def toString: String = s"MAP($typeInfo)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[MapRelDataType]

  override def equals(other: Any): Boolean = other match {
    case that: MapRelDataType =>
      super.equals(that) &&
        (that canEqual this) &&
        keyType == that.keyType &&
        valueType == that.valueType &&
        isNullable == that.isNullable
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(keyType, valueType)
  }

}
