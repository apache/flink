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

package org.apache.flink.api.table.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.{SerializerTestInstance, TypeSerializer}
import org.apache.flink.api.table.Row
import org.junit.Assert._
import org.junit.Test

class RowSerializerTest {

  class RowSerializerTestInstance(serializer: TypeSerializer[Row],
                                  testData: Array[Row])
    extends SerializerTestInstance(serializer, classOf[Row], -1, testData: _*) {

    override protected def deepEquals(message: String, should: Row, is: Row): Unit = {
      val arity = should.productArity
      assertEquals(message, arity, is.productArity)
      var index = 0
      while (index < arity) {
        val copiedValue: Any = should.productElement(index)
        val element: Any = is.productElement(index)
        assertEquals(message, element, copiedValue)
        index += 1
      }
    }
  }

  @Test
  def testRowSerializer(): Unit ={

    val rowInfo: TypeInformation[Row] = new RowTypeInfo(
      Seq(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO), Seq("id", "name"))

    val row1 = new Row(2)
    row1.setField(0, 1)
    row1.setField(1, "a")

    val row2 = new Row(2)
    row2.setField(0, 2)
    row2.setField(1, null)

    val testData: Array[Row] = Array(row1, row2)

    val rowSerializer: TypeSerializer[Row] = rowInfo.createSerializer(new ExecutionConfig)

    val testInstance = new RowSerializerTestInstance(rowSerializer,testData)

    testInstance.testAll()
  }

}
