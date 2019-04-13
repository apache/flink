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

import java.io._
import java.net.{URL, URLClassLoader}

import org.apache.flink.api.common.typeutils.{TypeSerializerSchemaCompatibility, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.util.TestLogger
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}
import org.junit.Assert._
import org.scalatest.junit.JUnitSuiteLike

import scala.reflect.NameTransformer
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.{GenericRunnerSettings, Global}

class EnumValueSerializerUpgradeTest extends TestLogger with JUnitSuiteLike {

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder = _tempFolder

  val enumName = "EnumValueSerializerUpgradeTestEnum"

  val enumA =
    s"""
      |object $enumName extends Enumeration {
      |  val A, B, C = Value
      |}
    """.stripMargin

  val enumB =
    s"""
       |object $enumName extends Enumeration {
       |  val A, B, C, D = Value
       |}
    """.stripMargin

  val enumC =
    s"""
       |object $enumName extends Enumeration {
       |  val A, C = Value
       |}
    """.stripMargin

  val enumD =
    s"""
       |object $enumName extends Enumeration {
       |  val A, C, B = Value
       |}
    """.stripMargin

  val enumE =
    s"""
       |object $enumName extends Enumeration {
       |  val A = Value(42)
       |  val B = Value(5)
       |  val C = Value(1337)
       |}
    """.stripMargin

  /**
    * Check that identical enums don't require migration
    */
  @Test
  def checkIdenticalEnums(): Unit = {
    assertTrue(checkCompatibility(enumA, enumA).isCompatibleAsIs)
  }

  /**
    * Check that appending fields to the enum does not require migration
    */
  @Test
  def checkAppendedField(): Unit = {
    assertTrue(checkCompatibility(enumA, enumB).isCompatibleAsIs)
  }

  /**
    * Check that removing enum fields makes the snapshot incompatible.
    */
  @Test
  def checkRemovedField(): Unit = {
    assertTrue(checkCompatibility(enumA, enumC).isIncompatible)
  }

  /**
    * Check that changing the enum field order makes the snapshot incompatible.
    */
  @Test
  def checkDifferentFieldOrder(): Unit = {
    assertTrue(checkCompatibility(enumA, enumD).isIncompatible)
  }

  /**
    * Check that changing the enum ids causes a migration
    */
  @Test
  def checkDifferentIds(): Unit = {
    assertTrue(
      "Different ids should be incompatible.",
      checkCompatibility(enumA, enumE).isIncompatible)
  }

  def checkCompatibility(enumSourceA: String, enumSourceB: String)
    : TypeSerializerSchemaCompatibility[Enumeration#Value] = {
    import EnumValueSerializerUpgradeTest._

    val classLoader = compileAndLoadEnum(tempFolder.newFolder(), s"$enumName.scala", enumSourceA)

    val enum = instantiateEnum[Enumeration](classLoader, enumName)

    val enumValueSerializer = new EnumValueSerializer(enum)
    val snapshot = enumValueSerializer.snapshotConfiguration()

    val baos = new ByteArrayOutputStream()
    val output = new DataOutputViewStreamWrapper(baos)
    TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
      output, snapshot, enumValueSerializer)

    output.close()
    baos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val input=  new DataInputViewStreamWrapper(bais)

    val classLoader2 = compileAndLoadEnum(tempFolder.newFolder(), s"$enumName.scala", enumSourceB)

    val snapshot2 = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
      input,
      classLoader2,
      enumValueSerializer)
    val enum2 = instantiateEnum[Enumeration](classLoader2, enumName)

    val enumValueSerializer2 = new EnumValueSerializer(enum2)
    snapshot2.resolveSchemaCompatibility(enumValueSerializer2)
  }
}

object EnumValueSerializerUpgradeTest {
  def compileAndLoadEnum(root: File, filename: String, source: String): ClassLoader = {
    val file = writeSourceFile(root, filename, source)

    compileScalaFile(file)

    new URLClassLoader(
      Array[URL](root.toURI.toURL),
      Thread.currentThread().getContextClassLoader)
  }

  def instantiateEnum[T <: Enumeration](classLoader: ClassLoader, enumName: String): T = {
    val clazz = classLoader.loadClass(enumName + "$").asInstanceOf[Class[_ <: Enumeration]]
    val field = clazz.getField(NameTransformer.MODULE_INSTANCE_NAME)

    field.get(null).asInstanceOf[T]
  }

  def writeSourceFile(root: File, filename: String, source: String): File = {
    val file = new File(root, filename)
    val fileWriter = new FileWriter(file)

    fileWriter.write(source)

    fileWriter.close()

    file
  }

  def compileScalaFile(file: File): Unit = {
    val in = new BufferedReader(new StringReader(""))
    val out = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(System.out)))

    val settings = new GenericRunnerSettings(out.println _)

    // use the java classpath so that scala libraries are available to the compiler
    settings.usejavacp.value = true
    settings.outdir.value = file.getParent

    val reporter = new ConsoleReporter(settings)
    val global = new Global(settings, reporter)
    val run = new global.Run

    run.compile(List(file.getAbsolutePath))

    reporter.printSummary()
  }
}

