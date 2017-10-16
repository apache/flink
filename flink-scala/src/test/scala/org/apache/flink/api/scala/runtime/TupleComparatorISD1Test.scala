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
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeComparator}
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.runtime.tuple.base.TupleComparatorTestBase

class TupleComparatorISD1Test extends TupleComparatorTestBase[(Int, String, Double)] {

  protected def createComparator(ascending: Boolean): TypeComparator[(Int, String, Double)] = {
    val ti = createTypeInformation[(Int, String, Double)]
    ti.asInstanceOf[TupleTypeInfoBase[(Int, String, Double)]]
      .createComparator(Array(0), Array(ascending), 0, new ExecutionConfig)
  }

  protected def createSerializer: TypeSerializer[(Int, String, Double)] = {
    val ti = createTypeInformation[(Int, String, Double)]
    ti.createSerializer(new ExecutionConfig)
  }

  protected def getSortedTestData: Array[(Int, String, Double)] = {
    dataISD
  }

  private val dataISD = Array(
    (4, "hello", 20.0),
    (5, "hello", 23.2),
    (6, "world", 20.0),
    (7, "hello", 20.0),
    (8, "hello", 23.2),
    (9, "world", 20.0),
    (10, "hello", 20.0),
    (11, "hello", 23.2)
  )
}

