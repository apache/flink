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
package org.apache.flink.table.runtime.functions.tablefunctions

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType}
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, GenericRow}

import scala.annotation.varargs
import scala.collection.mutable
import scala.collection.immutable

class MultiKeyValue extends TableFunction[BaseRow] {

  // Use fast mode if and only if all field are constant string and firstSep contains only 1
  // character and secondSep contains 1 character
  private var canUseFastMode = true

  // field name to the position in output row
  private var fieldToIndex: immutable.Map[String, Array[Int]] = null

  @varargs
  def eval(
    sourceField: String,
    firstSep: String,
    secondSep: String,
    keyFieldNames: String*): Unit = {
    val keyFieldNum = keyFieldNames.size
    val row = new GenericRow(keyFieldNum)
    // If sourceField, each field in output row is null
    if (sourceField == null) {
      collect(row)
      return
    }
    if (canUseFastMode) {
      val sourceFieldLen = sourceField.length
      var index = 0
      var currentKeyValue: String = null
      while (index < sourceFieldLen) {
        var nextKeyValueLoc = sourceField.indexOf(firstSep, index)
        if (nextKeyValueLoc == -1) {
          nextKeyValueLoc = sourceFieldLen
        }
        currentKeyValue = sourceField.substring(index, nextKeyValueLoc)
        val item = currentKeyValue.split(secondSep, 2)
        if (item.length == 2) {
          fieldToIndex.get(item(0)) match {
            case Some(indice) => indice.foreach(row.update(_, BinaryString.fromString(item(1))))
            case _ => // ignore
          }
        }
        index = nextKeyValueLoc + firstSep.length // move to next KV
      }
    } else {
      val kvs = mutable.HashMap.empty[String, String]
      StringUtils.split(sourceField, firstSep).foreach(
        kv => {
          val item = StringUtils.split(kv, secondSep)
          if (item != null && item.length == 2) {
            kvs.put(item(0), item(1))
          }
        })
      keyFieldNames.zipWithIndex.foreach { case (key, index) =>
        if (key != null) {
          row.update(index, BinaryString.fromString(kvs.get(key).orNull))
        }
      }
    }
    collect(row)
  }

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = {
    val numOfNonFieldArgs = 3
    val keyFieldsNum = arguments.length - numOfNonFieldArgs
    val fieldArgToIndex = mutable.HashMap.empty[String, mutable.ArrayBuffer[Int]]
    arguments.take(numOfNonFieldArgs).drop(1).foreach { split =>
      if (split == null) {
        canUseFastMode = false
      } else if (split.toString.size != 1) {
        canUseFastMode = false
      }
    }
    arguments.drop(numOfNonFieldArgs).zipWithIndex.foreach { case (arg, index) =>
      if (arg == null) {
        canUseFastMode = false
      } else {
        val fieldName = arg.toString
        val keyFieldIndice = fieldArgToIndex.get(fieldName) match {
          case Some(indice) => indice
          case _ => mutable.ArrayBuffer[Int]()
        }
        keyFieldIndice += index
        fieldArgToIndex += (fieldName -> keyFieldIndice)
      }
    }
    if (canUseFastMode) {
      fieldToIndex = immutable.Map(fieldArgToIndex.map(f => (f._1 -> f._2.toArray)).toList: _*)
    }
    val keyFieldsValueType = (0 until keyFieldsNum).map(i => DataTypes.STRING).toArray[DataType]
    new RowType(keyFieldsValueType: _*)
  }

}
