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

package org.apache.flink.table.calcite

import org.apache.calcite.rel.`type`.RelDataTypeSystemImpl
import org.apache.calcite.sql.`type`.SqlTypeName

/**
  * Custom type system for Flink.
  */
class FlinkTypeSystem extends RelDataTypeSystemImpl {

  // we cannot use Int.MaxValue because of an overflow in Calcite's type inference logic
  // half should be enough for all use cases
  override def getMaxNumericScale: Int = Int.MaxValue / 2

  // we cannot use Int.MaxValue because of an overflow in Calcite's type inference logic
  // half should be enough for all use cases
  override def getMaxNumericPrecision: Int = Int.MaxValue / 2

  override def getDefaultPrecision(typeName: SqlTypeName): Int = typeName match {

    // Calcite will limit the length of the VARCHAR field to 65536
    case SqlTypeName.VARCHAR =>
      Int.MaxValue

    // we currently support only timestamps with milliseconds precision
    case SqlTypeName.TIMESTAMP =>
      3

    case _ =>
      super.getDefaultPrecision(typeName)
  }

}
