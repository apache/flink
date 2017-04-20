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
package org.apache.flink.table.functions

import java.nio.charset.Charset
import java.util

import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlTypeFamily, SqlTypeName}
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.LeafExpression

object EventTimeExtractor extends SqlFunction("ROWTIME", SqlKind.OTHER_FUNCTION,
  ReturnTypes.explicit(TimeModeTypes.ROWTIME), null, OperandTypes.NILADIC,
  SqlFunctionCategory.SYSTEM) {
  override def getSyntax: SqlSyntax = SqlSyntax.FUNCTION

  override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity =
    SqlMonotonicity.INCREASING
}

object ProcTimeExtractor extends SqlFunction("PROCTIME", SqlKind.OTHER_FUNCTION,
  ReturnTypes.explicit(TimeModeTypes.PROCTIME), null, OperandTypes.NILADIC,
  SqlFunctionCategory.SYSTEM) {
  override def getSyntax: SqlSyntax = SqlSyntax.FUNCTION

  override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity =
    SqlMonotonicity.INCREASING
}

abstract class TimeIndicator extends LeafExpression {
  /**
    * Returns the [[org.apache.flink.api.common.typeinfo.TypeInformation]]
    * for evaluating this expression.
    * It is sometimes not available until the expression is valid.
    */
  override private[flink] def resultType = SqlTimeTypeInfo.TIMESTAMP

  /**
    * Convert Expression to its counterpart in Calcite, i.e. RexNode
    */
  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) =
    throw new TableException("indicator functions (e.g. proctime() and rowtime()" +
      " are not executable. Please check your expressions.")
}

case class RowTime() extends TimeIndicator
case class ProcTime() extends TimeIndicator

object TimeModeTypes {

  // indicator data type for row time (event time)
  val ROWTIME = new RowTimeType
  // indicator data type for processing time
  val PROCTIME = new ProcTimeType

}

class RowTimeType extends TimeModeType {

  override def toString(): String = "ROWTIME"
  override def getFullTypeString: String = "ROWTIME_INDICATOR"
}

class ProcTimeType extends TimeModeType {

  override def toString(): String = "PROCTIME"
  override def getFullTypeString: String = "PROCTIME_INDICATOR"
}

abstract class TimeModeType extends RelDataType {

  override def getComparability: RelDataTypeComparability = RelDataTypeComparability.NONE

  override def isStruct: Boolean = false

  override def getFieldList: util.List[RelDataTypeField] = null

  override def getFieldNames: util.List[String] = null

  override def getFieldCount: Int = 0

  override def getStructKind: StructKind = StructKind.NONE

  override def getField(
     fieldName: String,
     caseSensitive: Boolean,
     elideRecord: Boolean): RelDataTypeField = null

  override def isNullable: Boolean = false

  override def getComponentType: RelDataType = null

  override def getKeyType: RelDataType = null

  override def getValueType: RelDataType = null

  override def getCharset: Charset = null

  override def getCollation: SqlCollation = null

  override def getIntervalQualifier: SqlIntervalQualifier = null

  override def getPrecision: Int = -1

  override def getScale: Int = -1

  override def getSqlTypeName: SqlTypeName = SqlTypeName.TIMESTAMP

  override def getSqlIdentifier: SqlIdentifier = null

  override def getFamily: RelDataTypeFamily = SqlTypeFamily.NUMERIC

  override def getPrecedenceList: RelDataTypePrecedenceList = ???

  override def isDynamicStruct: Boolean = false

}
