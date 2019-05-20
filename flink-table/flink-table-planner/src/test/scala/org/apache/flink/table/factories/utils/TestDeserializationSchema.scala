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

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row

/**
  * Deserialization schema for testing purposes.
  */
class TestDeserializationSchema(val typeInfo: TypeInformation[Row])
  extends DeserializationSchema[Row] {

  override def deserialize(message: Array[Byte]): Row = throw new UnsupportedOperationException()

  override def isEndOfStream(nextElement: Row): Boolean = throw new UnsupportedOperationException()

  override def getProducedType: TypeInformation[Row] = typeInfo

  def canEqual(other: Any): Boolean = other.isInstanceOf[TestDeserializationSchema]

  override def equals(other: Any): Boolean = other match {
    case that: TestDeserializationSchema =>
      (that canEqual this) &&
        typeInfo == that.typeInfo
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(typeInfo)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
