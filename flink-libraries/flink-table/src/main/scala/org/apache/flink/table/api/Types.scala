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
package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, TypeInformation, Types => JTypes}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, MultisetTypeInfo, ObjectArrayTypeInfo}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

import _root_.scala.annotation.varargs

/**
  * This class enumerates all supported types of the Table API & SQL.
  */
object Types {

  /**
    * Returns type information for a Table API string or SQL VARCHAR type.
    */
  val STRING: TypeInformation[_] = JTypes.STRING

  /**
    * Returns type information for a Table API boolean or SQL BOOLEAN type.
    */
  val BOOLEAN: TypeInformation[_] = JTypes.BOOLEAN

  /**
    * Returns type information for a Table API byte or SQL TINYINT type.
    */
  val BYTE: TypeInformation[_] = JTypes.BYTE

  /**
    * Returns type information for a Table API short or SQL SMALLINT type.
    */
  val SHORT: TypeInformation[_] = JTypes.SHORT

  /**
    * Returns type information for a Table API integer or SQL INT/INTEGER type.
    */
  val INT: TypeInformation[_] = JTypes.INT

  /**
    * Returns type information for a Table API long or SQL BIGINT type.
    */
  val LONG: TypeInformation[_] = JTypes.LONG

  /**
    * Returns type information for a Table API float or SQL FLOAT/REAL type.
    */
  val FLOAT: TypeInformation[_] = JTypes.FLOAT

  /**
    * Returns type information for a Table API integer or SQL DOUBLE type.
    */
  val DOUBLE: TypeInformation[_] = JTypes.DOUBLE

  /**
    * Returns type information for a Table API big decimal or SQL DECIMAL type.
    */
  val DECIMAL: TypeInformation[_] = JTypes.BIG_DEC

  /**
    * Returns type information for a Table API SQL date or SQL DATE type.
    */
  val SQL_DATE = JTypes.SQL_DATE

  /**
    * Returns type information for a Table API SQL time or SQL TIME type.
    */
  val SQL_TIME = JTypes.SQL_TIME

  /**
    * Returns type information for a Table API SQL timestamp or SQL TIMESTAMP type.
    */
  val SQL_TIMESTAMP = JTypes.SQL_TIMESTAMP

  /**
    * Returns type information for a Table API interval of months.
    */
  val INTERVAL_MONTHS = TimeIntervalTypeInfo.INTERVAL_MONTHS

  /**
    * Returns type information for a Table API interval milliseconds.
    */
  val INTERVAL_MILLIS = TimeIntervalTypeInfo.INTERVAL_MILLIS

  /**
    * Returns type information for [[org.apache.flink.types.Row]] with fields of the given types.
    *
    * A row is a variable-length, null-aware composite type for storing multiple values in a
    * deterministic field order. Every field can be null independent of the field's type.
    * The type of row fields cannot be automatically inferred; therefore, it is required to pass
    * type information whenever a row is used.
    *
    * <p>The schema of rows can have up to <code>Integer.MAX_VALUE</code> fields, however, all row instances
    * must have the same length otherwise serialization fails or information is lost.
    *
    * This method generates type information with fields of the given types; the fields have
    * the default names (f0, f1, f2 ..).
    *
    * @param types The types of the row fields, e.g., Types.STRING, Types.INT
    */
  @varargs
  def ROW(types: TypeInformation[_]*): TypeInformation[_] = {
    JTypes.ROW(types: _*)
  }

  /**
    * Returns type information for [[org.apache.flink.types.Row]] with fields of the given types
    * and with given names.
    *
    * A row is a variable-length, null-aware composite type for storing multiple values in a
    * deterministic field order. Every field can be null independent of the field's type.
    * The type of row fields cannot be automatically inferred; therefore, it is required to pass
    * type information whenever a row is used.
    *
    * <p>The schema of rows can have up to <code>Integer.MAX_VALUE</code> fields, however, all row instances
    * must have the same length otherwise serialization fails or information is lost.
    *
    * Example use: `Types.ROW(Array("name", "number"), Array(Types.STRING, Types.INT))`.
    *
    * @param fieldNames array of field names
    * @param types      array of field types
    */
  def ROW(fieldNames: Array[String], types: Array[TypeInformation[_]]): TypeInformation[_] = {
    JTypes.ROW_NAMED(fieldNames, types: _*)
  }

  /**
    * Generates type information for an array consisting of Java primitive elements. The elements
    * do not support null values.
    *
    * @param elementType type of the array elements; e.g. Types.INT
    */
  def PRIMITIVE_ARRAY(elementType: TypeInformation[_]): TypeInformation[_] = {
    elementType match {
      case BOOLEAN =>  PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO
      case BYTE =>  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
      case SHORT =>  PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO
      case INT =>  PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO
      case LONG =>  PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO
      case FLOAT =>  PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO
      case DOUBLE =>  PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO
      case _ =>
        throw new TableException(s"$elementType cannot be an element of a primitive array." +
            s"Only Java primitive types are supported.")
    }
  }

  /**
    * Generates type information for an array consisting of Java object elements. Null values for
    * elements are supported.
    *
    * @param elementType type of the array elements; e.g. Types.STRING or Types.INT
    */
  def OBJECT_ARRAY(elementType: TypeInformation[_]): TypeInformation[_] = {
    ObjectArrayTypeInfo.getInfoFor(elementType)
  }

  /**
    * Generates type information for a Java HashMap.
    *
    * @param keyType type of the keys of the map e.g. Types.STRING
    * @param valueType type of the values of the map e.g. Types.STRING
    */
  def MAP(keyType: TypeInformation[_], valueType: TypeInformation[_]): TypeInformation[_] = {
    new MapTypeInfo(keyType, valueType)
  }

  /**
    * Generates type information for a Multiset.
    *
    * @param elementType type of the elements of the multiset e.g. Types.STRING
    */
  def MULTISET(elementType: TypeInformation[_]): TypeInformation[_] = {
    new MultisetTypeInfo(elementType)
  }
}
