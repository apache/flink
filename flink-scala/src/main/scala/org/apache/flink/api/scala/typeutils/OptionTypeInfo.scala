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

import org.apache.flink.annotation.{Public, PublicEvolving, VisibleForTesting}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}

import scala.collection.JavaConverters._

/**
 * TypeInformation for [[Option]].
 */
@Public
class OptionTypeInfo[A, T <: Option[A]](private val elemTypeInfo: TypeInformation[A])
  extends TypeInformation[T] with AtomicType[T] {

  @PublicEvolving
  override def isBasicType: Boolean = false
  @PublicEvolving
  override def isTupleType: Boolean = false
  @PublicEvolving
  override def isKeyType: Boolean = elemTypeInfo.isKeyType
  @PublicEvolving
  override def getTotalFields: Int = 1
  @PublicEvolving
  override def getArity: Int = 1
  @PublicEvolving
  override def getTypeClass = classOf[Option[_]].asInstanceOf[Class[T]]
  @PublicEvolving
  override def getGenericParameters = Map[String, TypeInformation[_]]("A" -> elemTypeInfo).asJava

  @PublicEvolving
  override def createComparator(ascending: Boolean, executionConfig: ExecutionConfig) = {
    if (isKeyType) {
      val elemCompartor = elemTypeInfo.asInstanceOf[AtomicType[A]]
        .createComparator(ascending, executionConfig)
      new OptionTypeComparator[A](ascending, elemCompartor).asInstanceOf[TypeComparator[T]]
    } else {
      throw new UnsupportedOperationException("Element type that doesn't support ")
    }
  }

  @PublicEvolving
  def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] = {
    if (elemTypeInfo == null) {
      // this happens when the type of a DataSet is None, i.e. DataSet[None]
      new OptionSerializer(new NothingSerializer).asInstanceOf[TypeSerializer[T]]
    } else {
      new OptionSerializer(elemTypeInfo.createSerializer(executionConfig))
        .asInstanceOf[TypeSerializer[T]]
    }
  }

  override def toString = s"Option[$elemTypeInfo]"

  override def equals(obj: Any): Boolean = {
    obj match {
      case optTpe: OptionTypeInfo[_, _] =>
        optTpe.canEqual(this) && elemTypeInfo.equals(optTpe.elemTypeInfo)
      case _ => false
    }
  }

  def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[OptionTypeInfo[_, _]]
  }

  override def hashCode: Int = {
    elemTypeInfo.hashCode()
  }

  @VisibleForTesting
  def getElemTypeInfo: TypeInformation[A] = elemTypeInfo
}
