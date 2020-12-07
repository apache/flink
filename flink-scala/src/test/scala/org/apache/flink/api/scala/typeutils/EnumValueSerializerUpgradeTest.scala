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

import java.util

import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase.TestSpecification
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerMatchers, TypeSerializerSchemaCompatibility, TypeSerializerUpgradeTestBase}
import org.apache.flink.testutils.migration.MigrationVersion
import org.hamcrest.Matcher
import org.hamcrest.Matchers.is
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
  * A [[TypeSerializerUpgradeTestBase]] for [[EnumValueSerializer]].
 */
@RunWith(classOf[Parameterized])
class EnumValueSerializerUpgradeTest(
  spec: TestSpecification[Letters.Value, Letters.Value])
extends TypeSerializerUpgradeTestBase[Letters.Value, Letters.Value](spec) {}

object EnumValueSerializerUpgradeTest {

  private val supplier =
    new util.function.Supplier[EnumValueSerializer[Letters.type]] {
      override def get(): EnumValueSerializer[Letters.type] =
        new EnumValueSerializer(Letters)
    }

  @Parameterized.Parameters(name = "Test Specification = {0}")
  def testSpecifications(): util.Collection[TestSpecification[_, _]] = {
    val testSpecifications =
      new util.ArrayList[TypeSerializerUpgradeTestBase.TestSpecification[_, _]]

    for (migrationVersion <- TypeSerializerUpgradeTestBase.MIGRATION_VERSIONS) {
      testSpecifications.add(
        new TypeSerializerUpgradeTestBase.TestSpecification[Letters.Value, Letters.Value](
        "scala-enum-serializer",
          migrationVersion,
          classOf[EnumValueSerializerSetup],
          classOf[EnumValueSerializerVerifier]))
    }

    testSpecifications
  }

  /**
   * This class is only public to work with
   * [[org.apache.flink.api.common.typeutils.ClassRelocator]].
   */
  final class EnumValueSerializerSetup
      extends TypeSerializerUpgradeTestBase.PreUpgradeSetup[Letters.Value] {
    override def createPriorSerializer: TypeSerializer[Letters.Value] = supplier.get()

    override def createTestData: Letters.Value = Letters.A
  }

  final class EnumValueSerializerVerifier extends
      TypeSerializerUpgradeTestBase.UpgradeVerifier[Letters.Value] {
    override def createUpgradedSerializer: TypeSerializer[Letters.Value] = supplier.get()

    override def testDataMatcher: Matcher[Letters.Value] = is(Letters.A)

    override def schemaCompatibilityMatcher(version: MigrationVersion):
        Matcher[TypeSerializerSchemaCompatibility[Letters.Value]] =
      TypeSerializerMatchers.isCompatibleAsIs[Letters.Value]()
  }
}
