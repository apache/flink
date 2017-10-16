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
import org.apache.flink.types.Row

import _root_.scala.annotation.varargs

/**
  * This class enumerates all supported types of the Table API.
  */
object Types {

  val STRING = JTypes.STRING
  val BOOLEAN = JTypes.BOOLEAN

  val BYTE = JTypes.BYTE
  val SHORT = JTypes.SHORT
  val INT = JTypes.INT
  val LONG = JTypes.LONG
  val FLOAT = JTypes.FLOAT
  val DOUBLE = JTypes.DOUBLE
  val DECIMAL = JTypes.DECIMAL

  val SQL_DATE = JTypes.SQL_DATE
  val SQL_TIME = JTypes.SQL_TIME
  val SQL_TIMESTAMP = JTypes.SQL_TIMESTAMP
  val INTERVAL_MONTHS = TimeIntervalTypeInfo.INTERVAL_MONTHS
  val INTERVAL_MILLIS = TimeIntervalTypeInfo.INTERVAL_MILLIS

  /**
    * Generates row type information.
    *
    * A row type consists of zero or more fields with a field name and a corresponding type.
    *
    * The fields have the default names (f0, f1, f2 ..).
    *
    * @param types types of row fields; e.g. Types.STRING, Types.INT
    */
  @varargs
  def ROW(types: TypeInformation[_]*): TypeInformation[Row] = {
    JTypes.ROW(types: _*)
  }

  /**
    * Generates row type information.
    *
    * A row type consists of zero or more fields with a field name and a corresponding type.
    *
    * @param names names of row fields, e.g. "userid", "name"
    * @param types types of row fields; e.g. Types.STRING, Types.INT
    */
  def ROW(names: Array[String], types: Array[TypeInformation[_]]): TypeInformation[Row] = {
    JTypes.ROW_NAMED(names, types: _*)
  }

  /**
    * Generates type information for an array consisting of Java primitive elements.
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
    * Generates type information for an array consisting of Java object elements.
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
