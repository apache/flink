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
package org.apache.flink.table.util

import org.apache.flink.table.api.TableException
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.logical.{LogicalType, RowType, TypeInformationAnyType}
import org.apache.flink.table.types.{ClassLogicalTypeConverter, DataType}
import org.apache.flink.types.Row

import java.lang.reflect.Modifier

import scala.collection.JavaConversions._

// TODO remove this, and use org.apache.flink.table.typeutils.FieldInfoUtils
object FieldInfoUtils {

  /**
    * Returns field names for a given [[DataType]].
    *
    * @param inputType The DataType extract the field names.
    * @tparam A The type of the DataType.
    * @return An array holding the field names
    */
  def getFieldNames[A](inputType: DataType): Array[String] = {
    validateType(inputType)

    val fieldNames: Array[String] = fromDataTypeToLogicalType(inputType) match {
      case t: RowType => t.getFieldNames.toArray(Array[String]())
      case t => Array("f0")
    }

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    fieldNames
  }

  /**
    * Returns field indexes for a given [[DataType]].
    *
    * @param inputType The DataType extract the field positions from.
    * @return An array holding the field positions
    */
  def getFieldIndices(inputType: DataType): Array[Int] = {
    getFieldNames(inputType).indices.toArray
  }

  /**
    * Returns field types for a given [[DataType]].
    *
    * @param inputType The DataType to extract field types from.
    * @return An array holding the field types.
    */
  def getFieldTypes(inputType: DataType): Array[LogicalType] = {
    validateType(inputType)

    fromDataTypeToLogicalType(inputType) match {
      case t: RowType => t.getChildren.toArray(Array[LogicalType]())
      case t => Array(t)
    }
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param dataType type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateType(dataType: DataType): Unit = {
    var clazz = dataType.getConversionClass
    if (clazz == null) {
      clazz = ClassLogicalTypeConverter.getDefaultExternalClassForType(dataType.getLogicalType)
    }
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw new TableException(
        s"Class '$clazz' described in type information '$dataType' must be " +
          s"static and globally accessible.")
    }
  }

  /**
    * Returns field names and field positions for a given [[DataType]].
    *
    * @param inputType The DataType extract the field names and positions from.
    * @tparam A The type of the DataType.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected[flink] def getFieldInfo[A](
      inputType: DataType): (Array[String], Array[Int]) = {

    val logicalType = fromDataTypeToLogicalType(inputType)
    logicalType match {
      case value: TypeInformationAnyType[A]
        if value.getTypeInformation.getTypeClass == classOf[Row] =>
        throw new TableException(
          "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
            "Please specify the type of the input with a RowTypeInfo.")
      case _ =>
        (getFieldNames(inputType), getFieldIndices(inputType))
    }
  }

  /**
    * Returns field names and field positions for a given [[DataType]] and [[Array]] of
    * field names. It does not handle time attributes.
    *
    * @param inputType The [[DataType]] against which the field names are referenced.
    * @param fields The fields that define the field names.
    * @tparam A The type of the DataType.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  // TODO: we should support Expression fields after we introduce [Expression]
  // TODO remove this method and use FieldInfoUtils#getFieldsInfo
  protected[flink] def getFieldInfo[A](
      inputType: DataType,
      fields: Array[String]): (Array[String], Array[Int]) = {

    validateType(inputType)

    def referenceByName(name: String, ct: RowType): Option[Int] = {
      val inputIdx = ct.getFieldIndex(name)
      if (inputIdx < 0) {
        throw new TableException(s"$name is not a field of type $ct. " +
          s"Expected: ${ct.getFieldNames.mkString(", ")}")
      } else {
        Some(inputIdx)
      }
    }

    val indexedNames: Array[(Int, String)] = fromDataTypeToLogicalType(inputType) match {

      case g: TypeInformationAnyType[A]
        if g.getTypeInformation.getTypeClass == classOf[Row] ||
          g.getTypeInformation.getTypeClass == classOf[BaseRow] =>
        throw new TableException(
          "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
            "Please specify the type of the input with a RowTypeInfo.")

      case t: RowType =>

        // determine schema definition mode (by position or by name)
        val isRefByPos = isReferenceByPosition(t, fields)

        fields.zipWithIndex flatMap { case (name, idx) =>
          if (name.endsWith("rowtime") || name.endsWith("proctime")) {
            None
          } else {
            if (isRefByPos) {
              Some((idx, name))
            } else {
              referenceByName(name, t).map((_, name))
            }
          }
        }

      case _ => // atomic or other custom type information
        if (fields.length > 1) {
          // only accept the first field for an atomic type
          throw new TableException("Only accept one field to reference an atomic type.")
        }
        // first field reference is mapped to atomic type
        Array((0, fields(0)))
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames, fieldIndexes)
  }

  /**
    * Reference input fields by name:
    * All fields in the schema definition are referenced by name
    * (and possibly renamed using an alias (as). In this mode, fields can be reordered and
    * projected out. Moreover, we can define proctime and rowtime attributes at arbitrary
    * positions using arbitrary names (except those that exist in the result schema). This mode
    * can be used for any input type, including POJOs.
    *
    * Reference input fields by position:
    * In this mode, fields are simply renamed. Event-time attributes can
    * replace the field on their position in the input data (if it is of correct type) or be
    * appended at the end. Proctime attributes must be appended at the end. This mode can only be
    * used if the input type has a defined field order (tuple, case class, Row) and no of fields
    * references a field of the input type.
    */
  // TODO: we should support Expression fields after we introduce [Expression]
  protected def isReferenceByPosition(ct: RowType, fields: Array[String]): Boolean = {
    val inputNames = ct.getFieldNames

    // Use the by-position mode if no of the fields exists in the input.
    // This prevents confusing cases like ('f2, 'f0, 'myName) for a Tuple3 where fields are renamed
    // by position but the user might assume reordering instead of renaming.
    fields.forall(!inputNames.contains(_))
  }

}
