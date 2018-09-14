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

package org.apache.flink.streaming.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{createTuple2TypeInformation => apiTupleCreator}
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo, TypeUtils}
import org.apache.flink.streaming.api.datastream.{ DataStream => JavaStream }
import org.apache.flink.streaming.api.datastream.{ SplitStream => SplitJavaStream }
import org.apache.flink.streaming.api.datastream.{ ConnectedStreams => ConnectedJavaStreams }
import org.apache.flink.streaming.api.datastream.{ BroadcastConnectedStream => BroadcastConnectedJavaStreams }
import org.apache.flink.streaming.api.datastream.{ KeyedStream => KeyedJavaStream }

import language.implicitConversions
import language.experimental.macros

package object scala {
  
  // We have this here so that we always have generated TypeInformationS when
  // using the Scala API
  implicit def createTypeInformation[T]: TypeInformation[T] = macro TypeUtils.createTypeInfo[T]

  /**
   * Converts an [[org.apache.flink.streaming.api.datastream.DataStream]] to a
   * [[org.apache.flink.streaming.api.scala.DataStream]].
   */
  private[flink] def asScalaStream[R](stream: JavaStream[R])
                                             = new DataStream[R](stream)

  /**
   * Converts an [[org.apache.flink.streaming.api.datastream.KeyedStream]] to a
   * [[org.apache.flink.streaming.api.scala.KeyedStream]].
   */
  private[flink] def asScalaStream[R, K](stream: KeyedJavaStream[R, K])
                                             = new KeyedStream[R, K](stream)

  /**
   * Converts an [[org.apache.flink.streaming.api.datastream.SplitStream]] to a
   * [[org.apache.flink.streaming.api.scala.SplitStream]].
   */
  private[flink] def asScalaStream[R](stream: SplitJavaStream[R])
                                             = new SplitStream[R](stream)
  /**
   * Converts an [[org.apache.flink.streaming.api.datastream.ConnectedStreams]] to a
   * [[org.apache.flink.streaming.api.scala.ConnectedStreams]].
   */
  private[flink] def asScalaStream[IN1, IN2](stream: ConnectedJavaStreams[IN1, IN2])
                                             = new ConnectedStreams[IN1, IN2](stream)
  /**
    * Converts an [[org.apache.flink.streaming.api.datastream.BroadcastConnectedStream]] to a
    * [[org.apache.flink.streaming.api.scala.BroadcastConnectedStream]].
    */
  private[flink] def asScalaStream[IN1, IN2](stream: BroadcastConnectedJavaStreams[IN1, IN2])
                                              = new BroadcastConnectedStream[IN1, IN2](stream)

  private[flink] def fieldNames2Indices(
      typeInfo: TypeInformation[_],
      fields: Array[String]): Array[Int] = {
    typeInfo match {
      case ti: CaseClassTypeInfo[_] =>
        val result = ti.getFieldIndices(fields)

        if (result.contains(-1)) {
          throw new IllegalArgumentException("Fields '" + fields.mkString(", ") +
            "' are not valid for '" + ti.toString + "'.")
        }

        result

      case _ =>
        throw new UnsupportedOperationException("Specifying fields by name is only" +
          "supported on Case Classes (for now).")
    }
  }

  def createTuple2TypeInformation[T1, T2](
      t1: TypeInformation[T1],
      t2: TypeInformation[T2]) : TypeInformation[(T1, T2)] =
    apiTupleCreator[T1, T2](t1, t2)
}
