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

package org.apache.flink.api.table.plan

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.JoinRelType._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.table.{Row, TableException}

import scala.collection.JavaConversions._

object TypeConverter {

  def typeInfoToSqlType(typeInfo: TypeInformation[_]): SqlTypeName = typeInfo match {
    case BOOLEAN_TYPE_INFO => BOOLEAN
    case BOOLEAN_VALUE_TYPE_INFO => BOOLEAN
    case BYTE_TYPE_INFO => TINYINT
    case BYTE_VALUE_TYPE_INFO => TINYINT
    case SHORT_TYPE_INFO => SMALLINT
    case SHORT_VALUE_TYPE_INFO => SMALLINT
    case INT_TYPE_INFO => INTEGER
    case INT_VALUE_TYPE_INFO => INTEGER
    case LONG_TYPE_INFO => BIGINT
    case LONG_VALUE_TYPE_INFO => BIGINT
    case FLOAT_TYPE_INFO => FLOAT
    case FLOAT_VALUE_TYPE_INFO => FLOAT
    case DOUBLE_TYPE_INFO => DOUBLE
    case DOUBLE_VALUE_TYPE_INFO => DOUBLE
    case STRING_TYPE_INFO => VARCHAR
    case STRING_VALUE_TYPE_INFO => VARCHAR
    case DATE_TYPE_INFO => DATE

    case CHAR_TYPE_INFO | CHAR_VALUE_TYPE_INFO =>
      throw new TableException("Character type is not supported.")

//    case t: TupleTypeInfo[_] => ROW
//    case c: CaseClassTypeInfo[_] => ROW
//    case p: PojoTypeInfo[_] => STRUCTURED
//    case g: GenericTypeInfo[_] => OTHER

    case t@_ =>
      throw new TableException(s"Type is not supported: $t")
  }

  def sqlTypeToTypeInfo(sqlType: SqlTypeName): TypeInformation[_] = sqlType match {
    case BOOLEAN => BOOLEAN_TYPE_INFO
    case TINYINT => BYTE_TYPE_INFO
    case SMALLINT => SHORT_TYPE_INFO
    case INTEGER => INT_TYPE_INFO
    case BIGINT => LONG_TYPE_INFO
    case FLOAT => FLOAT_TYPE_INFO
    case DOUBLE => DOUBLE_TYPE_INFO
    case VARCHAR | CHAR => STRING_TYPE_INFO
    case DATE => DATE_TYPE_INFO
    case _ =>
      println(sqlType)
      ??? // TODO more types
  }

  def determineReturnType(
      logicalRowType: RelDataType,
      expectedPhysicalType: Option[TypeInformation[Any]],
      nullable: Boolean,
      useEfficientTypes: Boolean)
    : TypeInformation[Any] = {
    // convert to type information
    val logicalFieldTypes = logicalRowType.getFieldList map { relDataType =>
      TypeConverter.sqlTypeToTypeInfo(relDataType.getType.getSqlTypeName)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames

    val returnType = expectedPhysicalType match {
      // a certain physical type is expected (but not Row)
      // check if expected physical type is compatible with logical field type
      case Some(typeInfo) if typeInfo.getTypeClass != classOf[Row] =>
        if (typeInfo.getArity != logicalFieldTypes.length) {
          throw new TableException("Arity of result does not match expected type.")
        }
        typeInfo match {

          // POJO type expected
          case pt: PojoTypeInfo[_] =>
            logicalFieldTypes.zipWithIndex foreach {
              case (fieldTypeInfo, i) =>
                val fieldName = logicalFieldNames(i)
                val index = pt.getFieldIndex(fieldName)
                if (index < 0) {
                  throw new TableException(s"POJO does not define field name: $fieldName")
                }
                val expectedTypeInfo = pt.getTypeAt(i)
                if (fieldTypeInfo != expectedTypeInfo) {
                  throw new TableException(s"Result field does not match expected type. " +
                    s"Expected: $expectedTypeInfo; Actual: $fieldTypeInfo")
                }
            }

          // Tuple/Case class type expected
          case ct: CompositeType[_] =>
            logicalFieldTypes.zipWithIndex foreach {
              case (fieldTypeInfo, i) =>
                val expectedTypeInfo = ct.getTypeAt(i)
                if (fieldTypeInfo != expectedTypeInfo) {
                  throw new TableException(s"Result field does not match expected type. " +
                    s"Expected: $expectedTypeInfo; Actual: $fieldTypeInfo")
                }
            }

          // Atomic type expected
          case at: AtomicType[_] =>
            val fieldTypeInfo = logicalFieldTypes.head
            if (fieldTypeInfo != at) {
              throw new TableException(s"Result field does not match expected type. " +
                s"Expected: $at; Actual: $fieldTypeInfo")
            }

          case _ =>
            throw new TableException("Unsupported result type.")
        }
        typeInfo

      // Row is expected, create the arity for it
      case Some(typeInfo) if typeInfo.getTypeClass == classOf[Row] =>
        new RowTypeInfo(logicalFieldTypes)

      // no physical type
      // determine type based on logical fields and configuration parameters
      case None =>
        // no need for efficient types -> use Row
        // we cannot use efficient types if row arity > tuple arity or nullable
        if (!useEfficientTypes || logicalFieldTypes.length > Tuple.MAX_ARITY || nullable) {
          new RowTypeInfo(logicalFieldTypes)
        }
        // use efficient type tuple or atomic type
        else {
          if (logicalFieldTypes.length == 1) {
            logicalFieldTypes.head
          }
          else {
            new TupleTypeInfo[Tuple](logicalFieldTypes.toArray:_*)
          }
        }
    }
    returnType.asInstanceOf[TypeInformation[Any]]
  }

  def sqlJoinTypeToFlinkJoinType(sqlJoinType: JoinRelType): JoinType = sqlJoinType match {
    case INNER => JoinType.INNER
    case LEFT => JoinType.LEFT_OUTER
    case RIGHT => JoinType.RIGHT_OUTER
    case FULL => JoinType.FULL_OUTER
  }
}
