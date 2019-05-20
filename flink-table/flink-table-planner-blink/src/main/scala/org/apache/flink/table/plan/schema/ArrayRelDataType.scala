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

import org.apache.flink.table.`type`.ArrayType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.ArraySqlType

/**
  * Flink distinguishes between primitive arrays (int[], double[], ...) and
  * object arrays (Integer[], MyPojo[], ...). This custom type supports both cases.
  */
class ArrayRelDataType(
    val arrayType: ArrayType,
    elementType: RelDataType,
    isNullable: Boolean)
  extends ArraySqlType(
    elementType,
    isNullable) {

  override def toString = s"ARRAY($arrayType)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[ArrayRelDataType]

  override def equals(other: Any): Boolean = other match {
    case that: ArrayRelDataType =>
      super.equals(that) &&
        (that canEqual this) &&
          arrayType == that.arrayType &&
        isNullable == that.isNullable
    case _ => false
  }

  override def hashCode(): Int = {
    arrayType.hashCode()
  }

}
