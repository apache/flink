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
package org.apache.flink.api.scala.runtime

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.{ComparatorTestBase, TypeComparator, TypeSerializer}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.EnumValueTypeInfo


class EnumValueComparatorTest extends ComparatorTestBase[Suit.Value] {

  protected def createComparator(ascending: Boolean): TypeComparator[Suit.Value] = {
    val ti = createTypeInformation[Suit.Value]
    ti.asInstanceOf[EnumValueTypeInfo[Suit.type]].createComparator(ascending, new ExecutionConfig)
  }

  protected def createSerializer: TypeSerializer[Suit.Value] = {
    val ti = createTypeInformation[Suit.Value]
    ti.createSerializer(new ExecutionConfig)
  }

  protected def getSortedTestData: Array[Suit.Value] = {
    dataISD
  }

  private val dataISD = Array(
    Suit.Clubs,
    Suit.Diamonds,
    Suit.Hearts,
    Suit.Spades
  )
}

object Suit extends Enumeration {
  type Suit = Value
  val Clubs, Diamonds, Hearts, Spades = Value
}
