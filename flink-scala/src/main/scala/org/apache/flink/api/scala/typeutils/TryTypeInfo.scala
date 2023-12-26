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
import scala.util.Try

/**
 * TypeInformation for [[scala.util.Try]].
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
class TryTypeInfo[A, T <: Try[A]](val elemTypeInfo: TypeInformation[A]) extends TypeInformation[T] {

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
  override def getTypeClass = classOf[Try[_]].asInstanceOf[Class[T]]
  @PublicEvolving
  override def getGenericParameters = Map[String, TypeInformation[_]]("T" -> elemTypeInfo).asJava

  @PublicEvolving
  def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] = {
    if (elemTypeInfo == null) {
      // this happens when the type of a DataSet is None, i.e. DataSet[Failure]
      new TrySerializer(new NothingSerializer, executionConfig).asInstanceOf[TypeSerializer[T]]
    } else {
      new TrySerializer(elemTypeInfo.createSerializer(executionConfig), executionConfig)
        .asInstanceOf[TypeSerializer[T]]
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case tryTypeInfo: TryTypeInfo[_, _] =>
        tryTypeInfo.canEqual(this) &&
        elemTypeInfo.equals(tryTypeInfo.elemTypeInfo)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[TryTypeInfo[_, _]]
  }

  override def hashCode(): Int = {
    elemTypeInfo.hashCode()
  }

  override def toString = s"Try[$elemTypeInfo]"
}
