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

import org.apache.flink.annotation.{PublicEvolving, Public}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer

import scala.collection.JavaConverters._

/**
 * TypeInformation [[Either]].
 */
@Public
class EitherTypeInfo[A, B, T <: Either[A, B]](
    val clazz: Class[T],
    val leftTypeInfo: TypeInformation[A],
    val rightTypeInfo: TypeInformation[B])
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
  override def getTypeClass = clazz
  @PublicEvolving
  override def getGenericParameters =
    Map[String, TypeInformation[_]]("A" -> leftTypeInfo, "B" -> rightTypeInfo).asJava

  @PublicEvolving
  def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] = {
    val leftSerializer = if (leftTypeInfo != null) {
      leftTypeInfo.createSerializer(executionConfig)
    } else {
      new NothingSerializer
    }

    val rightSerializer = if (rightTypeInfo != null) {
      rightTypeInfo.createSerializer(executionConfig)
    } else {
      new NothingSerializer
    }
    new EitherSerializer(leftSerializer, rightSerializer)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case eitherTypeInfo: EitherTypeInfo[_, _, _] =>
        eitherTypeInfo.canEqual(this) &&
        clazz.equals(eitherTypeInfo.clazz) &&
        leftTypeInfo.equals(eitherTypeInfo.leftTypeInfo) &&
        rightTypeInfo.equals(eitherTypeInfo.rightTypeInfo)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[EitherTypeInfo[_, _, _]]
  }

  override def hashCode(): Int = {
    31 * (31 * clazz.hashCode() + leftTypeInfo.hashCode()) + rightTypeInfo.hashCode()
  }

  override def toString = s"Either[$leftTypeInfo, $rightTypeInfo]"
}
