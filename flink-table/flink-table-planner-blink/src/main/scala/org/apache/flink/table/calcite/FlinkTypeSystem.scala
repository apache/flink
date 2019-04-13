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

import org.apache.flink.table.`type`.{DecimalType, InternalType, InternalTypes}
import org.apache.flink.table.typeutils.TypeCheckUtils

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeSystemImpl}
import org.apache.calcite.sql.`type`.SqlTypeName

/**
  * Custom type system for Flink.
  */
class FlinkTypeSystem extends RelDataTypeSystemImpl {

  // set the maximum precision of a NUMERIC or DECIMAL type to DecimalType.MAX_PRECISION.
  override def getMaxNumericPrecision: Int = DecimalType.MAX_PRECISION

  // the max scale can't be greater than precision
  override def getMaxNumericScale: Int = DecimalType.MAX_PRECISION

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

  // when union a number of CHAR types of different lengths, we should cast to a VARCHAR
  // this fixes the problem of CASE WHEN with different length string literals but get wrong
  // result with additional space suffix
  override def shouldConvertRaggedUnionTypesToVarying(): Boolean = true

  override def deriveAvgAggType(
      typeFactory: RelDataTypeFactory, argType: RelDataType): RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toInternalType(argType)
    val avgType = FlinkTypeSystem.deriveAvgAggType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createTypeFromInternalType(
      avgType, argType.isNullable)
  }

  override def deriveSumType(
      typeFactory: RelDataTypeFactory, argType: RelDataType): RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toInternalType(argType)
    val sumType = FlinkTypeSystem.deriveSumType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createTypeFromInternalType(
      sumType, argType.isNullable)
  }
}

object FlinkTypeSystem {

  def deriveAvgAggType(argType: InternalType): InternalType = argType match {
    case dt: DecimalType =>
      val result = DecimalType.inferAggAvgType(dt.scale())
      DecimalType.of(result.precision(), result.scale())
    case nt if TypeCheckUtils.isNumeric(nt) => InternalTypes.DOUBLE
    case _ =>
      throw new RuntimeException("Unsupported argType for AVG(): " + argType)
  }

  def deriveSumType(argType: InternalType): InternalType = argType match {
    case dt: DecimalType =>
      val result = DecimalType.inferAggSumType(dt.scale())
      DecimalType.of(result.precision(), result.scale())
    case nt if TypeCheckUtils.isNumeric(nt) =>
      argType
    case _ =>
      throw new RuntimeException("Unsupported argType for SUM(): " + argType)
  }

}
