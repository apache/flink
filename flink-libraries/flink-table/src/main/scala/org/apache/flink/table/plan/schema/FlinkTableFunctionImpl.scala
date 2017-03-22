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

import java.lang.reflect.{Method, Type}
import java.util

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.TableFunction
import org.apache.calcite.schema.impl.ReflectiveFunctionBase
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.{TableFunction => FlinkUDTF}

/**
  * This is heavily inspired by Calcite's [[org.apache.calcite.schema.impl.TableFunctionImpl]].
  * We need it in order to create a [[org.apache.flink.table.functions.utils.TableSqlFunction]].
  * The main difference is that we override the [[getRowType()]] and [[getElementType()]].
  *
  * @param tableFunction The Table Function instance
  * @param fieldIndexes The field indexes. If it is null, it will be inferred from
  *                     the [[tableFunction]]
  * @param fieldNames The field names. If it is null, it will be inferred from the
  *                   [[tableFunction]]
  * @param evalMethod The eval() method of the [[tableFunction]]
  */
class FlinkTableFunctionImpl[T](
    val tableFunction: FlinkUDTF[T],
    var fieldIndexes: Array[Int],
    var fieldNames: Array[String],
    val evalMethod: Method)
  extends ReflectiveFunctionBase(evalMethod)
  with TableFunction {

  checkFields()

  /**
    * Cached resultType
    */
  var resultType: TypeInformation[T] = _

  override def getElementType(arguments: util.List[AnyRef]): Type = classOf[Array[Object]]

  override def getRowType(typeFactory: RelDataTypeFactory,
                          arguments: util.List[AnyRef]): RelDataType = {

    // Get the result type from table function
    resultType = if (tableFunction.getResultType(arguments) != null) {
      tableFunction.getResultType(arguments)
    } else {
      TypeExtractor.createTypeInfo(
        tableFunction, classOf[FlinkUDTF[_]], tableFunction.getClass, 0)
        .asInstanceOf[TypeInformation[T]]
    }
    if (null == fieldNames || null == fieldIndexes) {
      val fieldInfo = UserDefinedFunctionUtils.getFieldInfo(resultType)
      fieldNames = fieldInfo._1
      fieldIndexes = fieldInfo._2
      checkFields()
    }

    val fieldTypes: Array[TypeInformation[_]] =
      resultType match {
        case cType: CompositeType[T] =>
          if (fieldNames.length != cType.getArity) {
            throw new TableException(
              s"Arity of type (" + cType.getFieldNames.deep + ") " +
                "not equal to number of field names " + fieldNames.deep + ".")
          }
          fieldIndexes.map(cType.getTypeAt(_).asInstanceOf[TypeInformation[_]])
        case aType: AtomicType[T] =>
          if (fieldIndexes.length != 1 || fieldIndexes(0) != 0) {
            throw new TableException(
              "Non-composite input type may have only a single field and its index must be 0.")
          }
          Array(aType)
      }

    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = flinkTypeFactory.builder
    fieldNames
      .zip(fieldTypes)
      .foreach { f =>
        builder.add(f._1, flinkTypeFactory.createTypeFromTypeInfo(f._2)).nullable(true)
      }
    builder.build
  }

  private def checkFields(): Unit = {

    if (null == fieldNames || null == fieldIndexes) {
      return
    }

    if (fieldIndexes.length != fieldNames.length) {
      throw new TableException(
        "Number of field indexes and field names must be equal.")
    }

    // check uniqueness of field names
    if (fieldNames.length != fieldNames.toSet.size) {
      throw new TableException(
        "Table field names must be unique.")
    }
  }
}
