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

package org.apache.flink.table.typeutils

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.JoinRelType._
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.types.Row
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory

import scala.collection.JavaConversions._

object TypeConverter {

  val DEFAULT_ROW_TYPE = new RowTypeInfo().asInstanceOf[TypeInformation[Any]]

  /**
    * Determines the return type of Flink operators based on the logical fields, the expected
    * physical type and configuration parameters.
    *
    * For example:
    *   - No physical type expected, only 3 non-null fields and efficient type usage enabled
    *       -> return Tuple3
    *   - No physical type expected, efficient type usage enabled, but 3 nullable fields
    *       -> return Row because Tuple does not support null values
    *   - Physical type expected
    *       -> check if physical type is compatible and return it
    *
    * @param logicalRowType logical row information
    * @param expectedPhysicalType expected physical type
    * @param nullable fields can be nullable
    * @param useEfficientTypes use the most efficient types (e.g. Tuples and value types)
    * @return suitable return type
    */
  def determineReturnType(
      logicalRowType: RelDataType,
      expectedPhysicalType: Option[TypeInformation[Any]],
      nullable: Boolean,
      useEfficientTypes: Boolean)
    : TypeInformation[Any] = {
    // convert to type information
    val logicalFieldTypes = logicalRowType.getFieldList map { relDataType =>
      FlinkTypeFactory.toTypeInfo(relDataType.getType)
    }
    // field names
    val logicalFieldNames = logicalRowType.getFieldNames.toList

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
            logicalFieldNames.zip(logicalFieldTypes) foreach {
              case (fName, fType) =>
                val pojoIdx = pt.getFieldIndex(fName)
                if (pojoIdx < 0) {
                  throw new TableException(s"POJO does not define field name: $fName")
                }
                val expectedTypeInfo = pt.getTypeAt(pojoIdx)
                if (fType != expectedTypeInfo) {
                  throw new TableException(s"Result field does not match expected type. " +
                    s"Expected: $expectedTypeInfo; Actual: $fType")
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
        new RowTypeInfo(logicalFieldTypes: _*)

      // no physical type
      // determine type based on logical fields and configuration parameters
      case None =>
        // no need for efficient types -> use Row
        // we cannot use efficient types if row arity > tuple arity or nullable
        if (!useEfficientTypes || logicalFieldTypes.length > Tuple.MAX_ARITY || nullable) {
          new RowTypeInfo(logicalFieldTypes: _*)
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

  def flinkJoinTypeToRelType(joinType: JoinType) = joinType match {
    case JoinType.INNER => JoinRelType.INNER
    case JoinType.LEFT_OUTER => JoinRelType.LEFT
    case JoinType.RIGHT_OUTER => JoinRelType.RIGHT
    case JoinType.FULL_OUTER => JoinRelType.FULL
  }
}
