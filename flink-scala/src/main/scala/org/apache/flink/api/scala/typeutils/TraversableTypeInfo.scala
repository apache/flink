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

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.collection.JavaConverters._

/**
 * TypeInformation for Scala Collections.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@Public
abstract class TraversableTypeInfo[T <: TraversableOnce[E], E](
    val clazz: Class[T],
    val elementTypeInfo: TypeInformation[E])
  extends TypeInformation[T] {

  @PublicEvolving
  override def isBasicType: Boolean = false
  @PublicEvolving
  override def isTupleType: Boolean = false
  @PublicEvolving
  override def isKeyType: Boolean = false
  @PublicEvolving
  override def getTotalFields: Int = 1
  @PublicEvolving
  override def getArity: Int = 1
  @PublicEvolving
  override def getTypeClass: Class[T] = clazz
  @PublicEvolving
  override def getGenericParameters =
    Map[String, TypeInformation[_]]("A" -> elementTypeInfo).asJava

  @PublicEvolving
  def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T]

  override def equals(other: Any): Boolean = {
    other match {
      case traversable: TraversableTypeInfo[_, _] =>
        traversable.canEqual(this) &&
        clazz == traversable.clazz &&
        elementTypeInfo.equals(traversable.elementTypeInfo)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    31 * clazz.hashCode() + elementTypeInfo.hashCode()
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[TraversableTypeInfo[_, _]]
  }

  override def toString = s"$clazz[$elementTypeInfo]"
}
