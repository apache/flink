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

@Public
class UnitTypeInfo extends TypeInformation[Unit] {
  @PublicEvolving
  override def isBasicType(): Boolean = false
  @PublicEvolving
  override def isTupleType(): Boolean = false
  @PublicEvolving
  override def getArity(): Int = 0
  @PublicEvolving
  override def getTotalFields(): Int = 0
  @PublicEvolving
  override def getTypeClass(): Class[Unit] = classOf[Unit]
  @PublicEvolving
  override def isKeyType(): Boolean = false

  @PublicEvolving
  override def createSerializer(config: ExecutionConfig): TypeSerializer[Unit] =
    (new UnitSerializer).asInstanceOf[TypeSerializer[Unit]]

  override def canEqual(obj: scala.Any): Boolean = {
    obj.isInstanceOf[UnitTypeInfo]
  }

  override def toString() = "UnitTypeInfo"

  override def equals(obj: scala.Any) = {
    obj.isInstanceOf[UnitTypeInfo]
  }

  override def hashCode() = classOf[UnitTypeInfo].hashCode
}
