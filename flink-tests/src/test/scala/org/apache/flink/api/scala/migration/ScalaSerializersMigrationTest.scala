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

package org.apache.flink.api.scala.migration

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils._
import org.junit.{Assert, Test}

import scala.util.Try

class ScalaSerializersMigrationTest {

  /**
   * Verifies that the generated classnames for anonymous Scala serializers remain the same.
   *
   * The classnames in this test are collected from running the same type information generation
   * code in previous version branches. They should not change across different Flink versions.
   */
  @Test
  def testStableAnonymousClassnameGeneration(): Unit = {
    val caseClassInfo = createTypeInformation[CustomCaseClass]
    val caseClassWithNestingInfo =
      createTypeInformation[CustomCaseClassWithNesting]
        .asInstanceOf[CaseClassTypeInfo[_]]
    val traversableInfo =
      createTypeInformation[List[CustomCaseClass]]
        .asInstanceOf[TraversableTypeInfo[_,_]]
    val tryInfo =
      createTypeInformation[Try[CustomCaseClass]]
        .asInstanceOf[TryTypeInfo[_,_]]
    val optionInfo =
      createTypeInformation[Option[CustomCaseClass]]
        .asInstanceOf[OptionTypeInfo[_,_]]
    val eitherInfo =
      createTypeInformation[Either[CustomCaseClass, String]]
        .asInstanceOf[EitherTypeInfo[_,_,_]]

    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$8",
      caseClassInfo.getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$8$$anon$1",
      caseClassInfo.createSerializer(new ExecutionConfig).getClass.getName)

    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$9",
      caseClassWithNestingInfo.getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$9$$anon$3",
      caseClassWithNestingInfo.createSerializer(new ExecutionConfig).getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$9$$anon$10",
      caseClassWithNestingInfo.getTypeAt("nested").getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$9$$anon$10$$anon$2",
      caseClassWithNestingInfo.getTypeAt("nested")
        .createSerializer(new ExecutionConfig).getClass.getName)

    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$16",
      traversableInfo.getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$16$$anon$12",
      traversableInfo.createSerializer(new ExecutionConfig).getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$11",
      traversableInfo.elementTypeInfo.getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$11$$anon$4",
      traversableInfo.elementTypeInfo.createSerializer(new ExecutionConfig).getClass.getName)

    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$13",
      tryInfo.elemTypeInfo.getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$13$$anon$5",
      tryInfo.elemTypeInfo.createSerializer(new ExecutionConfig).getClass.getName)

    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$14",
      optionInfo.getElemTypeInfo.getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$14$$anon$6",
      optionInfo.getElemTypeInfo.createSerializer(new ExecutionConfig).getClass.getName)

    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$15",
      eitherInfo.leftTypeInfo.getClass.getName)
    Assert.assertEquals(
      "org.apache.flink.api.scala.migration.ScalaSerializersMigrationTest$$anon$15$$anon$7",
      eitherInfo.leftTypeInfo.createSerializer(new ExecutionConfig).getClass.getName)
  }
}
