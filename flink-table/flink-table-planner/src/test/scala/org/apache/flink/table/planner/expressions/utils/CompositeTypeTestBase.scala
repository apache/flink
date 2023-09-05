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
package org.apache.flink.table.planner.expressions.utils

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.planner.expressions.utils.CompositeTypeTestBase.{MyCaseClass, MyCaseClass2, MyCaseClass3, MyPojo}
import org.apache.flink.types.Row

class CompositeTypeTestBase extends ExpressionTestBase {

  override def testData: Row = {
    val testData = new Row(14)
    testData.setField(0, MyCaseClass(42, "Bob", booleanField = true))
    testData.setField(1, MyCaseClass2(MyCaseClass(25, "Timo", booleanField = false)))
    testData.setField(2, ("a", "b"))
    testData.setField(3, new org.apache.flink.api.java.tuple.Tuple2[String, String]("a", "b"))
    testData.setField(4, new MyPojo())
    testData.setField(5, 13)
    testData.setField(6, MyCaseClass2(null))
    testData.setField(7, Tuple1(true))
    testData.setField(8, Array(Tuple2(true, 23), Tuple2(false, 12)))
    testData.setField(9, Array(Tuple1(true), null))
    testData.setField(10, Array(MyCaseClass(42, "Bob", booleanField = true)))
    testData.setField(11, Array(new MyPojo(), null))
    testData.setField(12, Array(MyCaseClass3(Array(MyCaseClass(42, "Alice", booleanField = true)))))
    testData.setField(13, Array(MyCaseClass2(MyCaseClass(42, "Bob", booleanField = true))))
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */ createTypeInformation[MyCaseClass],
      /* 1 */ createTypeInformation[MyCaseClass2],
      /* 2 */ createTypeInformation[(String, String)],
      /* 3 */ new TupleTypeInfo(Types.STRING, Types.STRING),
      /* 4 */ TypeExtractor.createTypeInfo(classOf[MyPojo]),
      /* 5 */ Types.INT,
      /* 6 */ TypeExtractor.createTypeInfo(classOf[MyCaseClass2]),
      /* 7 */ createTypeInformation[Tuple1[Boolean]],
      /* 8 */ createTypeInformation[Array[Tuple2[Boolean, Int]]],
      /* 9 */ createTypeInformation[Array[Tuple1[Boolean]]],
      /* 10 */ createTypeInformation[Array[MyCaseClass]],
      /* 11 */ createTypeInformation[Array[MyPojo]],
      /* 12 */ createTypeInformation[Array[MyCaseClass3]],
      /* 13 */ createTypeInformation[Array[MyCaseClass2]]
    )
  }
}

object CompositeTypeTestBase {
  case class MyCaseClass(intField: Int, stringField: String, booleanField: Boolean)

  case class MyCaseClass2(objectField: MyCaseClass)

  case class MyCaseClass3(arrayField: Array[MyCaseClass])

  class MyPojo {
    private var myInt: Int = 0
    private var myString: String = "Hello"

    def getMyInt: Int = myInt

    def setMyInt(value: Int): Unit = {
      myInt = value
    }

    def getMyString: String = myString

    def setMyString(value: String): Unit = {
      myString = value
    }
  }
}
