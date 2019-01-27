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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeSystemImpl}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, BigDecimalTypeInfo, NumericTypeInfo, TypeInformation}
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.typeutils.TypeCheckUtils

/**
  * Custom type system for Flink.
  */
class FlinkTypeSystem extends RelDataTypeSystemImpl {

  override def getMaxNumericScale: Int = Decimal.MAX_PS

  override def getMaxNumericPrecision: Int = Decimal.MAX_PS

  override def getDefaultPrecision(typeName: SqlTypeName): Int = typeName match {

    // Calcite will limit the length of the VARCHAR field to 65536
    case SqlTypeName.VARCHAR =>
      Int.MaxValue

    // we currenty support only timestamps with milliseconds precision
    case SqlTypeName.TIMESTAMP =>
      3

    case SqlTypeName.FLOAT =>
      7

    case _ =>
      super.getDefaultPrecision(typeName)
  }

  override def deriveAvgAggType(typeFactory: RelDataTypeFactory, argType: RelDataType)
  : RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toTypeInfo(argType)
    val avgType = FlinkTypeSystem.deriveAvgAggType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createTypeFromTypeInfo(avgType, argType.isNullable)
  }

  override def deriveSumType(typeFactory: RelDataTypeFactory, argType: RelDataType)
  : RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toTypeInfo(argType)
    val sumType = FlinkTypeSystem.deriveSumType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createTypeFromTypeInfo(sumType, argType.isNullable)
  }

  override def shouldConvertRaggedUnionTypesToVarying(): Boolean = true
}

object FlinkTypeSystem {
  def deriveAvgAggType(argType: TypeInformation[_])
  : TypeInformation[_] = argType match {
    case dt: BigDecimalTypeInfo =>
      val result = Decimal.inferAggAvgType(dt.precision(), dt.scale())
      BigDecimalTypeInfo.of(result.precision(), result.scale())
    case nt: NumericTypeInfo[_] =>
      BasicTypeInfo.DOUBLE_TYPE_INFO
    case _ =>
      throw new RuntimeException("Unsupported argType for AVG(): " + argType)
  }

  def deriveSumType(argType: TypeInformation[_])
  : TypeInformation[_] = argType match {
    case dt: BigDecimalTypeInfo =>
      val result = Decimal.inferAggSumType(dt.precision(), dt.scale())
      BigDecimalTypeInfo.of(result.precision(), result.scale())
    case nt: NumericTypeInfo[_] =>
      argType
    case _ =>
      throw new RuntimeException("Unsupported argType for SUM(): " + argType)
  }

  def deriveAvgAggType(argType: InternalType): InternalType = argType match {
    case dt: DecimalType =>
      val result = Decimal.inferAggAvgType(dt.precision(), dt.scale())
      DecimalType.of(result.precision(), result.scale())
    case nt if TypeCheckUtils.isNumeric(nt) => DataTypes.DOUBLE
    case _ =>
      throw new RuntimeException("Unsupported argType for AVG(): " + argType)
  }

  def deriveSumType(argType: InternalType): InternalType = argType match {
    case dt: DecimalType =>
      val result = Decimal.inferAggSumType(dt.precision(), dt.scale())
      DecimalType.of(result.precision(), result.scale())
    case nt if TypeCheckUtils.isNumeric(nt) =>
      argType
    case _ =>
      throw new RuntimeException("Unsupported argType for SUM(): " + argType)
  }
}
