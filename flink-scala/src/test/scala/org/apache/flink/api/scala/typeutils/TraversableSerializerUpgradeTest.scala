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
import java.util.function.Supplier

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase.TestSpecification
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerMatchers, TypeSerializerSchemaCompatibility, TypeSerializerUpgradeTestBase}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.testutils.migration.MigrationVersion
import org.hamcrest.Matcher
import org.hamcrest.Matchers.is
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.{BitSet, LinearSeq, mutable}

/**
 * A [[TypeSerializerUpgradeTestBase]] for [[TraversableSerializer]].
 */
@RunWith(classOf[Parameterized])
class TraversableSerializerUpgradeTest(
  testSpecification: TypeSerializerUpgradeTestBase.TestSpecification[
TraversableOnce[_], TraversableOnce[_]])
  extends TypeSerializerUpgradeTestBase[TraversableOnce[_], TraversableOnce[_]](testSpecification)

object TraversableSerializerUpgradeTest {

  object Types {

    class Pojo(var name: String, var count: Int) {
      def this() = this("", -1)

      override def equals(other: Any): Boolean = {
        other match {
          case oP: Pojo => name == oP.name && count == oP.count
          case _ => false
        }
      }
    }

    val seqTypeInfo = implicitly[TypeInformation[Seq[Int]]]
    val indexedSeqTypeInfo =
      implicitly[TypeInformation[IndexedSeq[Int]]]
    val linearSeqTypeInfo = implicitly[TypeInformation[LinearSeq[Int]]]
    val mapTypeInfo = implicitly[TypeInformation[Map[String, Int]]]
    val setTypeInfo = implicitly[TypeInformation[Set[Int]]]
    val bitsetTypeInfo = implicitly[TypeInformation[BitSet]]
    val mutableListTypeInfo =
      implicitly[TypeInformation[mutable.MutableList[Int]]]
    val seqTupleTypeInfo = implicitly[TypeInformation[Seq[(Int, String)]]]
    val seqPojoTypeInfo = implicitly[TypeInformation[Seq[Pojo]]]
  }

  import Types._

  @Parameterized.Parameters(name = "Test Specification = {0}")
  def testSpecifications: util.Collection[TestSpecification[_, _]] = {

    val testSpecifications =
      new util.ArrayList[TypeSerializerUpgradeTestBase.TestSpecification[_, _]]
    for (migrationVersion <- TypeSerializerUpgradeTestBase.MIGRATION_VERSIONS) {
      testSpecifications.add(
        new TestSpecification[BitSet, BitSet](
          "traversable-serializer-bitset",
          migrationVersion,
          classOf[BitsetSerializerSetup],
          classOf[BitsetSerializerVerifier]))
      testSpecifications.add(
        new TestSpecification[IndexedSeq[Int], IndexedSeq[Int]](
          "traversable-serializer-indexedseq",
          migrationVersion,
          classOf[IndexedSeqSerializerSetup],
          classOf[IndexedSeqSerializerVerifier]))
      testSpecifications.add(
        new TestSpecification[LinearSeq[Int], LinearSeq[Int]](
          "traversable-serializer-linearseq",
          migrationVersion,
          classOf[LinearSeqSerializerSetup],
          classOf[LinearSeqSerializerVerifier]))
      testSpecifications.add(
        new TestSpecification[Map[String, Int], Map[String, Int]](
          "traversable-serializer-map",
          migrationVersion,
          classOf[MapSerializerSetup],
          classOf[MapSerializerVerifier]))
      testSpecifications.add(
        new TestSpecification[mutable.MutableList[Int], mutable.MutableList[Int]](
          "traversable-serializer-mutable-list",
          migrationVersion,
          classOf[MutableListSerializerSetup],
          classOf[MutableListSerializerVerifier]))
      testSpecifications.add(
        new TestSpecification[Seq[Int], Seq[Int]](
          "traversable-serializer-seq",
          migrationVersion,
          classOf[SeqSerializerSetup],
          classOf[SeqSerializerVerifier]))
      testSpecifications.add(
        new TestSpecification[Set[Int], Set[Int]](
          "traversable-serializer-set",
          migrationVersion,
          classOf[SetSerializerSetup],
          classOf[SetSerializerVerifier]))
      testSpecifications.add(
        new TestSpecification[Seq[(Int, String)], Seq[(Int, String)]](
          "traversable-serializer-with-case-class",
          migrationVersion,
          classOf[SeqWithCaseClassSetup],
          classOf[SeqWithCaseClassVerifier]))
      testSpecifications.add(
        new TestSpecification[Seq[Pojo], Seq[Pojo]](
          "traversable-serializer-with-pojo",
          migrationVersion,
          classOf[SeqWithPojoSetup],
          classOf[SeqWithPojoVerifier]))
    }
    testSpecifications
  }

  final class BitsetSerializerSetup extends TypeSerializerUpgradeTestBase.PreUpgradeSetup[BitSet] {
    override def createPriorSerializer: TypeSerializer[BitSet] =
      new TypeSerializerSupplier(bitsetTypeInfo).get()

    override def createTestData: BitSet = BitSet(3, 2, 0)
  }

  final class BitsetSerializerVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[BitSet] {
    override def createUpgradedSerializer: TypeSerializer[BitSet] =
      new TypeSerializerSupplier(bitsetTypeInfo).get()

    override def testDataMatcher: Matcher[BitSet] = is(BitSet(3, 2, 0))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[BitSet]] =
      TypeSerializerMatchers.isCompatibleAsIs[BitSet]()
  }

  final class IndexedSeqSerializerSetup extends
    TypeSerializerUpgradeTestBase.PreUpgradeSetup[IndexedSeq[Int]] {
    override def createPriorSerializer: TypeSerializer[IndexedSeq[Int]] =
      new TypeSerializerSupplier(indexedSeqTypeInfo).get()

    override def createTestData: IndexedSeq[Int] = IndexedSeq(1, 2, 3)
  }

  final class IndexedSeqSerializerVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[IndexedSeq[Int]] {
    override def createUpgradedSerializer: TypeSerializer[IndexedSeq[Int]] =
      new TypeSerializerSupplier(indexedSeqTypeInfo).get()

    override def testDataMatcher: Matcher[IndexedSeq[Int]] = is(IndexedSeq(1, 2, 3))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[IndexedSeq[Int]]] =
      TypeSerializerMatchers.isCompatibleAsIs[IndexedSeq[Int]]()
  }

  final class LinearSeqSerializerSetup extends
    TypeSerializerUpgradeTestBase.PreUpgradeSetup[LinearSeq[Int]] {
    override def createPriorSerializer: TypeSerializer[LinearSeq[Int]] =
    new TypeSerializerSupplier(linearSeqTypeInfo).get()

    override def createTestData: LinearSeq[Int] = LinearSeq(2, 3, 4)
  }

  final class LinearSeqSerializerVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[LinearSeq[Int]] {
    override def createUpgradedSerializer: TypeSerializer[LinearSeq[Int]] =
      new TypeSerializerSupplier(linearSeqTypeInfo).get()

    override def testDataMatcher: Matcher[LinearSeq[Int]] = is(LinearSeq(2, 3, 4))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[LinearSeq[Int]]] =
      TypeSerializerMatchers.isCompatibleAsIs[LinearSeq[Int]]()
  }

  final class MapSerializerSetup extends
    TypeSerializerUpgradeTestBase.PreUpgradeSetup[Map[String, Int]] {
    override def createPriorSerializer: TypeSerializer[Map[String, Int]] =
      new TypeSerializerSupplier(mapTypeInfo).get()

    override def createTestData: Map[String, Int] = Map("Apache" -> 0, "Flink" -> 1)
  }

  final class MapSerializerVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[Map[String, Int]] {
    override def createUpgradedSerializer: TypeSerializer[Map[String, Int]] =
      new TypeSerializerSupplier(mapTypeInfo).get()

    override def testDataMatcher: Matcher[Map[String, Int]] = is(Map("Apache" -> 0, "Flink" -> 1))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[Map[String, Int]]] =
      TypeSerializerMatchers.isCompatibleAsIs[Map[String, Int]]()
  }

  final class MutableListSerializerSetup extends
    TypeSerializerUpgradeTestBase.PreUpgradeSetup[mutable.MutableList[Int]] {
    override def createPriorSerializer: TypeSerializer[mutable.MutableList[Int]] =
      new TypeSerializerSupplier(mutableListTypeInfo).get()

    override def createTestData: mutable.MutableList[Int] = mutable.MutableList(1, 2, 3)
  }

  final class MutableListSerializerVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[mutable.MutableList[Int]] {
    override def createUpgradedSerializer: TypeSerializer[mutable.MutableList[Int]] =
      new TypeSerializerSupplier(mutableListTypeInfo).get()

    override def testDataMatcher: Matcher[mutable.MutableList[Int]] =
      is(mutable.MutableList(1, 2, 3))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[mutable.MutableList[Int]]] =
      TypeSerializerMatchers.isCompatibleAsIs[mutable.MutableList[Int]]()
  }

  final class SeqSerializerSetup extends TypeSerializerUpgradeTestBase.PreUpgradeSetup[Seq[Int]] {
    override def createPriorSerializer: TypeSerializer[Seq[Int]] =
      new TypeSerializerSupplier(seqTypeInfo).get()

    override def createTestData: Seq[Int] = Seq(1, 2, 3)
  }

  final class SeqSerializerVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[Seq[Int]] {
    override def createUpgradedSerializer: TypeSerializer[Seq[Int]] =
      new TypeSerializerSupplier(seqTypeInfo).get()

    override def testDataMatcher: Matcher[Seq[Int]] = is(Seq(1, 2, 3))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
        Matcher[TypeSerializerSchemaCompatibility[Seq[Int]]] =
      TypeSerializerMatchers.isCompatibleAsIs[Seq[Int]]()
  }

  final class SetSerializerSetup extends TypeSerializerUpgradeTestBase.PreUpgradeSetup[Set[Int]] {
    override def createPriorSerializer: TypeSerializer[Set[Int]] =
      new TypeSerializerSupplier(setTypeInfo).get()

    override def createTestData: Set[Int] = Set(2, 3, 4)
  }

  final class SetSerializerVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[Set[Int]] {
    override def createUpgradedSerializer: TypeSerializer[Set[Int]] =
      new TypeSerializerSupplier(setTypeInfo).get()

    override def testDataMatcher: Matcher[Set[Int]] = is(Set(2, 3, 4))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
        Matcher[TypeSerializerSchemaCompatibility[Set[Int]]] =
      TypeSerializerMatchers.isCompatibleAsIs[Set[Int]]()
  }

  final class SeqWithCaseClassSetup extends
    TypeSerializerUpgradeTestBase.PreUpgradeSetup[Seq[(Int, String)]] {
    override def createPriorSerializer: TypeSerializer[Seq[(Int, String)]] =
      new TypeSerializerSupplier(seqTupleTypeInfo).get()

    override def createTestData: Seq[(Int, String)] = Seq((0, "Apache"), (1, "Flink"))
  }

  final class SeqWithCaseClassVerifier extends
    TypeSerializerUpgradeTestBase.UpgradeVerifier[Seq[(Int, String)]] {
    override def createUpgradedSerializer: TypeSerializer[Seq[(Int, String)]] =
      new TypeSerializerSupplier(seqTupleTypeInfo).get()

    override def testDataMatcher: Matcher[Seq[(Int, String)]] = is(Seq((0, "Apache"), (1, "Flink")))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[Seq[(Int, String)]]] =
      TypeSerializerMatchers.isCompatibleAsIs[Seq[(Int, String)]]()
  }

  final class SeqWithPojoSetup extends TypeSerializerUpgradeTestBase.PreUpgradeSetup[Seq[Pojo]] {
    override def createPriorSerializer: TypeSerializer[Seq[Pojo]] =
      new TypeSerializerSupplier(seqPojoTypeInfo).get()

    override def createTestData: Seq[Pojo] = Seq(new Pojo("Apache", 0), new Pojo("Flink", 1))
  }

  final class SeqWithPojoVerifier extends TypeSerializerUpgradeTestBase.UpgradeVerifier[Seq[Pojo]] {
    override def createUpgradedSerializer: TypeSerializer[Seq[Pojo]] =
      new TypeSerializerSupplier(seqPojoTypeInfo).get()

    override def testDataMatcher: Matcher[Seq[Pojo]] =
      is(Seq(new Pojo("Apache", 0), new Pojo("Flink", 1)))

    override def schemaCompatibilityMatcher(version: MigrationVersion):
    Matcher[TypeSerializerSchemaCompatibility[Seq[Pojo]]] =
      TypeSerializerMatchers.isCompatibleAsIs[Seq[Pojo]]()
  }

  private class TypeSerializerSupplier[T](typeInfo: TypeInformation[T])
    extends Supplier[TypeSerializer[T]] {
    override def get(): TypeSerializer[T] = {
      typeInfo
        .createSerializer(new ExecutionConfig)
        .asInstanceOf[TypeSerializer[T]]
    }
  }

}
