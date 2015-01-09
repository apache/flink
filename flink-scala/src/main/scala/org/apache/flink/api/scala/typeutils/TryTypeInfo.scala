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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.KryoSerializer

import scala.util.Try

/**
 * TypeInformation for [[scala.util.Try]].
 */
class TryTypeInfo[A, T <: Try[A]](elemTypeInfo: TypeInformation[A])
  extends TypeInformation[T] {

  override def isBasicType: Boolean = false
  override def isTupleType: Boolean = false
  override def isKeyType: Boolean = false
  override def getTotalFields: Int = 1
  override def getArity: Int = 1
  override def getTypeClass = classOf[Try[_]].asInstanceOf[Class[T]]

  def createSerializer(): TypeSerializer[T] = {
    if (elemTypeInfo == null) {
      // this happens when the type of a DataSet is None, i.e. DataSet[Failure]
      new TrySerializer(new NothingSerializer).asInstanceOf[TypeSerializer[T]]
    } else {
      new TrySerializer(elemTypeInfo.createSerializer()).asInstanceOf[TypeSerializer[T]]
    }
  }

  override def toString = s"Try[$elemTypeInfo]"
}
