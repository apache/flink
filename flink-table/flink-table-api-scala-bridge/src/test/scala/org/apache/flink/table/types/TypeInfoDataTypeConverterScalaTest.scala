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
package org.apache.flink.table.types

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.TypeInfoDataTypeConverterTest.TestSpec
import org.apache.flink.table.types.utils.DataTypeFactoryMock.dummyRaw
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util.stream

/** Scala tests for [[TypeInfoDataTypeConverter]]. */
class TypeInfoDataTypeConverterScalaTest {

  @ParameterizedTest
  @MethodSource(Array("testData"))
  def testScalaConversion(testSpec: TestSpec): Unit = {
    val dataType = TypeInfoDataTypeConverter.toDataType(testSpec.typeFactory, testSpec.typeInfo)
    assertThat(dataType).isEqualTo(testSpec.expectedDataType)
  }

}

object TypeInfoDataTypeConverterScalaTest {
  def testData(): stream.Stream[TestSpec] = java.util.stream.Stream.of(
    TestSpec
      .forType(createTypeInformation[ScalaCaseClass])
      .expectDataType(
        DataTypes
          .STRUCTURED(
            classOf[ScalaCaseClass],
            DataTypes.FIELD(
              "primitiveIntField",
              DataTypes.INT().notNull().bridgedTo(java.lang.Integer.TYPE)),
            DataTypes.FIELD("doubleField", DataTypes.DOUBLE().notNull()),
            DataTypes.FIELD("stringField", DataTypes.STRING().nullable())
          )
          .notNull()),
    TestSpec
      .forType(createTypeInformation[(java.lang.Double, java.lang.Float)])
      .expectDataType(
        DataTypes
          .STRUCTURED(
            classOf[(_, _)],
            DataTypes.FIELD("_1", DataTypes.DOUBLE().notNull()),
            DataTypes.FIELD("_2", DataTypes.FLOAT().notNull()))
          .notNull()),
    TestSpec
      .forType(createTypeInformation[Unit])
      .lookupExpects(classOf[Unit])
      .expectDataType(dummyRaw(classOf[Unit]))
  )

  // ----------------------------------------------------------------------------------------------
  // Test classes for extraction
  // ----------------------------------------------------------------------------------------------

  case class ScalaCaseClass(
      primitiveIntField: Int,
      doubleField: java.lang.Double,
      stringField: String
  )
}
