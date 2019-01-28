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

import java.lang.reflect.Type
import java.util
import java.util.Collections

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{FunctionParameter, TableFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory

/**
  * This is heavily inspired by Calcite's [[org.apache.calcite.schema.impl.TableFunctionImpl]].
  * We need it in order to create a [[org.apache.flink.table.functions.utils.TableSqlFunction]].
  * The main difference is that we override the [[getRowType()]] and [[getElementType()]].
  */
class FlinkTableFunctionImpl[T](
    val typeInfo: TypeInformation[T],
    val fieldIndexes: Array[Int],
    val fieldNames: Array[String])
  extends TableFunction {

  if (fieldIndexes.length != fieldNames.length) {
    throw new TableException(
      "Number of field indexes and field names must be equal.")
  }

  // check uniqueness of field names
  if (fieldNames.length != fieldNames.toSet.size) {
    throw new TableException(
      "Table field names must be unique.")
  }

  val fieldTypes: Array[TypeInformation[_]] =
    typeInfo match {

      case ct: CompositeType[T] =>
        if (fieldNames.length != ct.getArity) {
          throw new TableException(
            s"Arity of type (" + ct.getFieldNames.deep + ") " +
              "not equal to number of field names " + fieldNames.deep + ".")
        }
        fieldIndexes.map(ct.getTypeAt(_).asInstanceOf[TypeInformation[_]])

      case t: TypeInformation[T] =>
        if (fieldIndexes.length != 1 || fieldIndexes(0) != 0) {
          throw new TableException(
            "Non-composite input type may have only a single field and its index must be 0.")
        }
        Array(t)
    }

  override def getElementType(arguments: util.List[AnyRef]): Type = classOf[Array[Object]]

  // we do never use the FunctionParameters, so return an empty list
  override def getParameters: util.List[FunctionParameter] = Collections.emptyList()

  override def getRowType(typeFactory: RelDataTypeFactory,
                          arguments: util.List[AnyRef]): RelDataType = {
    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = flinkTypeFactory.builder
    fieldNames
      .zip(fieldTypes)
      .foreach { f =>
        builder.add(f._1, flinkTypeFactory.createTypeFromTypeInfo(f._2, isNullable = true))
      }
    builder.build
  }
}
