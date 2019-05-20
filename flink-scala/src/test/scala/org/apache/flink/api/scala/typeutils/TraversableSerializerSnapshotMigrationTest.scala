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
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshotMigrationTestBase}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.testutils.migration.MigrationVersion
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.{BitSet, LinearSeq, mutable}

/**
  * [[TraversableSerializer]] migration test.
  */
@RunWith(classOf[Parameterized])
class TraversableSerializerSnapshotMigrationTest(
  testSpecification: TypeSerializerSnapshotMigrationTestBase.TestSpecification[
    TraversableOnce[_]
  ]) extends TypeSerializerSnapshotMigrationTestBase[TraversableOnce[_]](
      testSpecification
    )

object TraversableSerializerSnapshotMigrationTest {

  object Types {

    class Pojo(var name: String, var count: Int) {
      def this() = this("", -1)

      override def equals(other: Any): Boolean = {
        other match {
          case oP: Pojo => name == oP.name && count == oP.count
          case _        => false
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

  @SuppressWarnings(Array("unchecked"))
  @Parameterized.Parameters(name = "Test Specification = {0}")
  def testSpecifications: util.Collection[
    TypeSerializerSnapshotMigrationTestBase.TestSpecification[_]] = {

    val testSpecifications: TypeSerializerSnapshotMigrationTestBase.TestSpecifications =
      new TypeSerializerSnapshotMigrationTestBase.TestSpecifications(
        MigrationVersion.v1_6,
        MigrationVersion.v1_7)

    val serializerSpecs = Seq(
      ("bitset", bitsetTypeInfo),
      ("indexedseq", indexedSeqTypeInfo),
      ("linearseq", linearSeqTypeInfo),
      ("map", mapTypeInfo),
      ("mutable-list", mutableListTypeInfo),
      ("seq", seqTypeInfo),
      ("set", setTypeInfo),
      ("with-case-class", seqTupleTypeInfo),
      ("with-pojo", seqPojoTypeInfo)
    )

    serializerSpecs foreach {
      case (data, typeInfo) =>
        testSpecifications.add(
          s"traversable-serializer-$data",
          classOf[TraversableSerializer[_, _]],
          classOf[TraversableSerializerSnapshot[_, _]],
          new TypeSerializerSupplier(typeInfo)
        )
    }

    testSpecifications.get
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
