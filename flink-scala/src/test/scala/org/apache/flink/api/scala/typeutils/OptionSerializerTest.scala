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

import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.common.typeutils.{SerializerTestBase, TypeSerializer}

import scala.util.Random

class OptionSerializerTest extends SerializerTestBase[Option[Integer]] {

  override protected def createSerializer(): TypeSerializer[Option[Integer]] =
    new OptionSerializer[Integer](new IntSerializer())

  override protected def getLength: Int = -1

  override protected def getTypeClass: Class[Option[Integer]] = classOf[Option[Integer]]

  override protected def getTestData: Array[Option[Integer]] = {
    val rnd = new Random(874597969123412341L)
    val rndInt = rnd.nextInt

    Array[Option[Integer]](
      Option(Integer.valueOf(0)),
      Option(Integer.valueOf(rndInt)),
      Option(Integer.valueOf(-rndInt)),
      Option.empty,
      null
    )
  }
}
