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

package org.apache.flink.table.types.extraction

import java.util

import org.apache.flink.table.annotation.{DataTypeHint, HintFlag}
import org.apache.flink.table.types.extraction.DataTypeExtractorScalaTest.{ScalaComplexPojo, ScalaPojoWithCustomFieldOrder, ScalaSimplePojo, ScalaSimplePojoWithManyAnnotations}
import org.apache.flink.table.types.extraction.DataTypeExtractorTest._
import org.junit.Test

/**
 * Scala tests for [[DataTypeExtractor]].
 */
class DataTypeExtractorScalaTest {

  val parameters: Seq[TestSpec] = Seq(
    // simple structured type without RAW type
    TestSpec
      .forType(classOf[ScalaSimplePojo])
      .expectDataType(getSimplePojoDataType(classOf[ScalaSimplePojo])),

    // complex nested structured type annotation on top of type
    TestSpec
      .forType(classOf[ScalaComplexPojo])
      .lookupExpects(classOf[Any])
      .expectDataType(getComplexPojoDataType(classOf[ScalaComplexPojo], classOf[ScalaSimplePojo])),

    // assigning constructor defines field order
    TestSpec
        .forType(classOf[ScalaPojoWithCustomFieldOrder])
        .expectDataType(getPojoWithCustomOrderDataType(classOf[ScalaPojoWithCustomFieldOrder])),

    // many annotations that partially override each other
    TestSpec
        .forType(classOf[ScalaSimplePojoWithManyAnnotations])
        .expectDataType(getSimplePojoDataType(classOf[ScalaSimplePojoWithManyAnnotations]))
  )

  @Test
  def testScalaExtraction(): Unit = {
    parameters.foreach(runExtraction)
  }
}

object DataTypeExtractorScalaTest {

  case class ScalaSimplePojo(
    intField: Integer,
    primitiveBooleanField: Boolean,
    primitiveIntField: Int,
    stringField: String
  )

  @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
  case class ScalaComplexPojo(
    var mapField: util.Map[String, Integer],
    var simplePojoField: ScalaSimplePojo,
    var someObject: Any
  )

  case class ScalaPojoWithCustomFieldOrder(
    z: java.lang.Long,
    y: java.lang.Boolean,
    x: java.lang.Integer
  )

  @DataTypeHint(forceRawPattern = Array("java.lang."))
  class ScalaSimplePojoWithManyAnnotations {
    @DataTypeHint("INT") var intField: Integer = _
    var primitiveBooleanField: Boolean = _
    @DataTypeHint(value = "INT NOT NULL", bridgedTo = classOf[Int]) var primitiveIntField: Any = _
    @DataTypeHint(forceRawPattern = Array()) var stringField: String = _
  }
}
