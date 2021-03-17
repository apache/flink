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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.MultisetSqlType
import org.apache.flink.api.common.typeinfo.TypeInformation

class MultisetRelDataType(
    val typeInfo: TypeInformation[_],
    elementType: RelDataType,
    isNullable: Boolean)
  extends MultisetSqlType(
    elementType,
    isNullable) {

  override def toString = s"MULTISET($elementType)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[MultisetRelDataType]

  override def equals(other: Any): Boolean = other match {
    case that: MultisetRelDataType =>
      super.equals(that) &&
        (that canEqual this) &&
        typeInfo == that.typeInfo &&
        isNullable == that.isNullable
    case _ => false
  }

  override def hashCode(): Int = {
    typeInfo.hashCode()
  }

  override def generateTypeString(sb: java.lang.StringBuilder, withDetail: Boolean): Unit = {
    sb.append(s"$elementType MULTISET($typeInfo)")
  }
}
