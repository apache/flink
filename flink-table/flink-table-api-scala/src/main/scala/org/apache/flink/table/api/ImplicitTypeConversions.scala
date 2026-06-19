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
package org.apache.flink.table.api

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.table.api.typeutils._
import org.apache.flink.table.expressions.Expression

import scala.language.experimental.macros
import scala.language.implicitConversions

/**
 * Implicit conversions from Scala literals to [[Expression]] and from [[Expression]] to
 * [[ImplicitExpressionOperations]].
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@Deprecated
@PublicEvolving
trait ImplicitTypeConversions {

  // We have this here so that we always have generated TypeInformationS when
  // using the Scala API
  implicit def createTypeInformation[T]: TypeInformation[T] = macro TypeUtils.createTypeInfo[T]

  // createTypeInformation does not fire for Nothing in some situations, which is probably
  // a compiler bug. The following line is a workaround for this.
  // (See TypeInformationGenTest.testNothingTypeInfoIsAvailableImplicitly)
  implicit val scalaNothingTypeInfo: TypeInformation[Nothing] = new ScalaNothingTypeInfo()

  def createTuple2TypeInformation[T1, T2](
      t1: TypeInformation[T1],
      t2: TypeInformation[T2]): TypeInformation[(T1, T2)] =
    new CaseClassTypeInfo[(T1, T2)](
      classOf[(T1, T2)],
      Array(t1, t2),
      Seq(t1, t2),
      Array("_1", "_2")) {

      override def createSerializer(
          serializerConfig: SerializerConfig): TypeSerializer[(T1, T2)] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity) {
          fieldSerializers(i) = types(i).createSerializer(serializerConfig)
        }

        new Tuple2CaseClassSerializer[T1, T2](classOf[(T1, T2)], fieldSerializers)
      }
    }
}
