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
package org.apache.flink.api.scala.typeutils

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

@Internal
@SerialVersionUID(5413377487955047394L)
class UnitSerializer extends TypeSerializerSingleton[Unit] {

  def isImmutableType(): Boolean = true

  def createInstance(): Unit = ()

  def copy(from: Unit): Unit = ()

  def copy(from: Unit, reuse: Unit): Unit = ()

  def getLength(): Int = 1

  def serialize(record: Unit, target: DataOutputView) {
    target.write(0)
  }

  def deserialize(source: DataInputView): Unit = {
    source.readByte()
    ()
  }

  def deserialize(reuse: Unit, source: DataInputView): Unit = {
    source.readByte()
    ()
  }

  def copy(source: DataInputView, target: DataOutputView) {
    target.write(source.readByte)
  }

  override def hashCode(): Int = classOf[UnitSerializer].hashCode

  override def canEqual(obj: scala.Any): Boolean = {
    obj.isInstanceOf[UnitSerializer]
  }
}
