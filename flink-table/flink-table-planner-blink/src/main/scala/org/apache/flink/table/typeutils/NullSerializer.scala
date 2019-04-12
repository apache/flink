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

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class NullSerializer extends TypeSerializerSingleton[Any] {

  override def isImmutableType = true

  override def createInstance(): Any = null

  override def copy(from: Any): Null = null

  override def copy(from: Any, reuse: Any): Null = null

  override def getLength = 0

  override def serialize(record: Any, target: DataOutputView): Unit = {
    target.writeByte(0)
  }

  override def deserialize(source: DataInputView): Any = {
    source.readByte()
    null
  }

  override def deserialize(reuse: Any, source: DataInputView): Any = {
    source.readByte()
    null
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
  }

  override def snapshotConfiguration() = throw new UnsupportedOperationException
}
