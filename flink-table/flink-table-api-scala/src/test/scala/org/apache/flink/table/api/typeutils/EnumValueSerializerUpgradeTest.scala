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
package org.apache.flink.table.api.typeutils

import org.apache.flink.FlinkVersion
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerConditions, TypeSerializerSchemaCompatibility, TypeSerializerUpgradeTestBase}
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase.TestSpecification
import org.apache.flink.table.api.typeutils.EnumValueSerializerUpgradeTest.{EnumValueSerializerSetup, EnumValueSerializerVerifier}
import org.apache.flink.test.util.MigrationTest

import org.assertj.core.api.Condition
import org.junit.jupiter.api.Disabled

import java.util
import java.util.Objects

/** A [[TypeSerializerUpgradeTestBase]] for [[EnumValueSerializer]]. */
@Disabled("FLINK-36334")
class EnumValueSerializerUpgradeTest
  extends TypeSerializerUpgradeTestBase[Letters.Value, Letters.Value] {

  override def createTestSpecifications(
      migrationVersion: FlinkVersion): util.Collection[TestSpecification[_, _]] = {
    val testSpecifications =
      new util.ArrayList[TypeSerializerUpgradeTestBase.TestSpecification[_, _]]

    testSpecifications.add(
      new TypeSerializerUpgradeTestBase.TestSpecification[Letters.Value, Letters.Value](
        "scala-enum-serializer",
        migrationVersion,
        classOf[EnumValueSerializerSetup],
        classOf[EnumValueSerializerVerifier]))

    testSpecifications
  }

  override def getMigrationVersions: util.Collection[FlinkVersion] = {
    FlinkVersion.rangeOf(FlinkVersion.v2_0, MigrationTest.getMostRecentlyPublishedVersion)
  }

}

object EnumValueSerializerUpgradeTest {

  private val supplier =
    new util.function.Supplier[EnumValueSerializer[Letters.type]] {
      override def get(): EnumValueSerializer[Letters.type] =
        new EnumValueSerializer(Letters)
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

  final class EnumValueSerializerVerifier
    extends TypeSerializerUpgradeTestBase.UpgradeVerifier[Letters.Value] {
    override def createUpgradedSerializer: TypeSerializer[Letters.Value] = supplier.get()

    override def testDataCondition: Condition[Letters.Value] =
      new Condition[Letters.Value]((l: Letters.Value) => Objects.equals(l, Letters.A), "is A")
    override def schemaCompatibilityCondition(
        version: FlinkVersion): Condition[TypeSerializerSchemaCompatibility[Letters.Value]] =
      TypeSerializerConditions.isCompatibleAsIs[Letters.Value]()
  }
}
