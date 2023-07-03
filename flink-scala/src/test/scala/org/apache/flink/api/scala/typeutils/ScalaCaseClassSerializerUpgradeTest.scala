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

import org.apache.flink.FlinkVersion
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerMatchers, TypeSerializerSchemaCompatibility, TypeSerializerUpgradeTestBase}
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase.TestSpecification
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.types.CustomCaseClass
import org.apache.flink.api.scala.typeutils.ScalaCaseClassSerializerUpgradeTest.{ScalaCaseClassSerializerSetup, ScalaCaseClassSerializerVerifier}

import org.hamcrest.Matcher
import org.hamcrest.Matchers.is

import java.util

/** A [[TypeSerializerUpgradeTestBase]] for [[ScalaCaseClassSerializer]]. */
class ScalaCaseClassSerializerUpgradeTest
  extends TypeSerializerUpgradeTestBase[CustomCaseClass, CustomCaseClass] {

  override def createTestSpecifications(
      migrationVersion: FlinkVersion): util.Collection[TestSpecification[_, _]] = {
    val testSpecifications =
      new util.ArrayList[TypeSerializerUpgradeTestBase.TestSpecification[_, _]]
    testSpecifications.add(
      new TypeSerializerUpgradeTestBase.TestSpecification[CustomCaseClass, CustomCaseClass](
        "scala-case-class-serializer",
        migrationVersion,
        classOf[ScalaCaseClassSerializerSetup],
        classOf[ScalaCaseClassSerializerVerifier]))

    testSpecifications
  }
}

object ScalaCaseClassSerializerUpgradeTest {

  private val typeInfo = createTypeInformation[CustomCaseClass]

  private val supplier =
    new util.function.Supplier[TypeSerializer[CustomCaseClass]] {
      override def get(): TypeSerializer[CustomCaseClass] =
        typeInfo.createSerializer(new ExecutionConfig)
    }

  /**
   * This class is only public to work with
   * [[org.apache.flink.api.common.typeutils.ClassRelocator]].
   */
  final class ScalaCaseClassSerializerSetup
    extends TypeSerializerUpgradeTestBase.PreUpgradeSetup[CustomCaseClass] {
    override def createPriorSerializer: TypeSerializer[CustomCaseClass] = supplier.get()

    override def createTestData: CustomCaseClass = CustomCaseClass("flink", 11)
  }

  final class ScalaCaseClassSerializerVerifier
    extends TypeSerializerUpgradeTestBase.UpgradeVerifier[CustomCaseClass] {
    override def createUpgradedSerializer: TypeSerializer[CustomCaseClass] = supplier.get()

    override def testDataMatcher: Matcher[CustomCaseClass] = is(CustomCaseClass("flink", 11))

    override def schemaCompatibilityMatcher(
        version: FlinkVersion): Matcher[TypeSerializerSchemaCompatibility[CustomCaseClass]] =
      TypeSerializerMatchers.isCompatibleAsIs[CustomCaseClass]()
  }
}
