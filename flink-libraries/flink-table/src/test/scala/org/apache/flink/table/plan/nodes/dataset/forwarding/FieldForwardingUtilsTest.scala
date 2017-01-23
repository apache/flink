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

package org.apache.flink.table.plan.nodes.dataset.forwarding

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.table.plan.nodes.dataset.forwarding.FieldForwardingUtils._
import org.junit.Assert._
import org.junit.Test

class FieldForwardingUtilsTest {

  @Test
  def testForwarding() = {
    val intType = BasicTypeInfo.INT_TYPE_INFO
    val strType = BasicTypeInfo.STRING_TYPE_INFO
    val doubleArrType = BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO

    val tuple = new TupleTypeInfo(intType, strType)
    val pojo = TypeExtractor.getForClass(classOf[TestPojo])

    val row = new RowTypeInfo(strType, intType, doubleArrType)
    val namedRow = new RowTypeInfo(
      Array[TypeInformation[_]](intType, doubleArrType, strType),
      Array("ints", "doubleArr", "strings")
    )

    assertEquals("f0->f1;f1->f0", getForwardedFields(tuple, row, Seq((0, 1), (1, 0))))
    assertEquals("f0->ints;f1->strings", getForwardedFields(tuple, namedRow, Seq((0, 0), (1, 2))))
    assertEquals("f0->someInt;f1->aString", getForwardedFields(tuple, pojo, Seq((0, 2), (1, 0))))

    assertEquals("*", getForwardedInput(intType, intType, Seq(0)))

    val customTypeWrapper = (info: TypeInformation[_]) =>
      info match {
        case array: BasicArrayTypeInfo[_, _] =>
          (_: Int) => s"*"
      }
    assertEquals("*", getForwardedInput(doubleArrType, doubleArrType, Seq(0), customTypeWrapper))
  }
}

//TODO can't test it in this package
case class TestCaseClass(aString: String, someInt: Int)

final class TestPojo {
  private var aString: String = _
  var doubleArray: Array[Double] = _
  var someInt: Int = 0

  def setaString(aString: String) =
    this.aString = aString

  def getaString: String = aString
}
