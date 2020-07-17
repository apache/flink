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

import java.util

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}

import scala.collection.JavaConverters._

class SealedTraitTypeInfo[T](
        clazz: Class[T],
        val subtypeClasses: Array[Class[_]],
        val subtypeTypeInfos: Array[TypeInformation[_]]) extends TypeInformation[T] {
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
  override def getGenericParameters: java.util.Map[String, TypeInformation[_]] =
    java.util.Map.of()

  @PublicEvolving
  def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] = {
    new SealedTraitSerializer[T](
      subtypeClasses,
      subtypeTypeInfos.map(_.createSerializer(executionConfig)))
  }

  override def toString = clazz.getName

  override def equals(obj: Any): Boolean = {
    obj match {
      case stTpe: SealedTraitTypeInfo[_] =>
        stTpe.canEqual(this) && subtypeClasses.sameElements(stTpe.subtypeClasses)
      case _ => false
    }
  }

  def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[SealedTraitTypeInfo[_]]
  }

  override def hashCode: Int = {
    util.Arrays.hashCode(subtypeTypeInfos.asInstanceOf[Array[AnyRef]])
  }

}
