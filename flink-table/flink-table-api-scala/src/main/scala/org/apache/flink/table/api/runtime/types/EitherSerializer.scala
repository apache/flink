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
package org.apache.flink.table.api.runtime.types

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

private class LeftSerializer[A, B] extends Serializer[Left[A, B]] {
  def write(kryo: Kryo, output: Output, left: Left[A, B]): Unit =
    kryo.writeClassAndObject(output, left.left.get)

  def read(kryo: Kryo, input: Input, cls: Class[_ <: Left[A, B]]): Left[A, B] =
    Left(kryo.readClassAndObject(input).asInstanceOf[A])
}

private class RightSerializer[A, B] extends Serializer[Right[A, B]] {
  def write(kryo: Kryo, output: Output, right: Right[A, B]): Unit =
    kryo.writeClassAndObject(output, right.right.get)

  def read(kryo: Kryo, input: Input, cls: Class[_ <: Right[A, B]]): Right[A, B] =
    Right(kryo.readClassAndObject(input).asInstanceOf[B])
}
