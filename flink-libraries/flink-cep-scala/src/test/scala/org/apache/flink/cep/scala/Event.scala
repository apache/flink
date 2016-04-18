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
package org.apache.flink.cep.scala

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.TypeExtractor

object Event {
  def createTypeSerializer: TypeSerializer[Event] = {
    val typeInformation: TypeInformation[Event] = TypeExtractor.createTypeInfo(classOf[Event])
    return typeInformation.createSerializer(new ExecutionConfig)
  }
}

class Event(var id: Int, var name: String, var price: Double) {

  override def toString: String = {
    "Event(" + id + ", " + name + ", " + price + ")"
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Event]

  override def equals(other: Any): Boolean = other match {
    case that: Event =>
      (that canEqual this) &&
        id == that.id &&
        name == that.name &&
        price == that.price
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, name, price)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
