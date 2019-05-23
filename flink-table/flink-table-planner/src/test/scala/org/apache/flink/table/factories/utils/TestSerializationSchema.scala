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

package org.apache.flink.table.factories.utils

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row

/**
  * Serialization schema for testing purposes.
  */
class TestSerializationSchema(val typeInfo: TypeInformation[Row]) extends SerializationSchema[Row] {

  override def serialize(element: Row): Array[Byte] = throw new UnsupportedOperationException()

  def canEqual(other: Any): Boolean = other.isInstanceOf[TestSerializationSchema]

  override def equals(other: Any): Boolean = other match {
    case that: TestSerializationSchema =>
      (that canEqual this) &&
        typeInfo == that.typeInfo
    case _ => false
  }

  override def hashCode(): Int = {
    31 * typeInfo.hashCode()
  }
}
