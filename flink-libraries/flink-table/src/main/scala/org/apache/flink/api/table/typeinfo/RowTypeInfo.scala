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
package org.apache.flink.api.table.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.table.Row
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo}

/**
 * TypeInformation for [[Row]].
 */
class RowTypeInfo(
    fieldTypes: Seq[TypeInformation[_]],
    fieldNames: Seq[String])
  extends CaseClassTypeInfo[Row](classOf[Row], Array(), fieldTypes, fieldNames) {

  def this(fields: Seq[Expression]) = this(fields.map(_.typeInfo), fields.map(_.name))

  if (fieldNames.toSet.size != fieldNames.size) {
    throw new IllegalArgumentException("Field names must be unique.")
  }

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[Row] = {
    val fieldSerializers: Array[TypeSerializer[Any]] = new Array[TypeSerializer[Any]](getArity)
    for (i <- 0 until getArity) {
      fieldSerializers(i) = this.types(i).createSerializer(executionConfig)
        .asInstanceOf[TypeSerializer[Any]]
    }

    new RowSerializer(fieldSerializers)
  }
}

