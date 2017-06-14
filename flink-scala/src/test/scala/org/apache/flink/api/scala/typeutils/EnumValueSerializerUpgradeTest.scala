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

import org.apache.flink.api.common.typeutils.{CompatibilityResult, TypeSerializerSerializationUtil}
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
      |@SerialVersionUID(1L)
      |object $enumName extends Enumeration {
      |  val A, B, C = Value
      |}
    """.stripMargin

  val enumB =
    s"""
       |@SerialVersionUID(1L)
       |object $enumName extends Enumeration {
       |  val A, B, C, D = Value
       |}
    """.stripMargin

  val enumC =
    s"""
       |@SerialVersionUID(1L)
       |object $enumName extends Enumeration {
       |  val A, C = Value
       |}
    """.stripMargin

  val enumD =
    s"""
       |@SerialVersionUID(1L)
       |object $enumName extends Enumeration {
       |  val A, C, B = Value
       |}
    """.stripMargin

  /**
    * Check that identical enums don't require migration
    */
  @Test
  def checkIdenticalEnums(): Unit = {
    assertFalse(checkCompatibility(enumA, enumA).isRequiresMigration)
  }

  /**
    * Check that appending fields to the enum does not require migration
    */
  @Test
  def checkAppendedField(): Unit = {
    assertFalse(checkCompatibility(enumA, enumB).isRequiresMigration)
  }

  /**
    * Check that removing enum fields requires migration
    */
  @Test
  def checkRemovedField(): Unit = {
    assertTrue(checkCompatibility(enumA, enumC).isRequiresMigration)
  }

  /**
    * Check that changing the enum field order requires migration
    */
  @Test
  def checkDifferentFieldOrder(): Unit = {
    assertTrue(checkCompatibility(enumA, enumD).isRequiresMigration)
  }

  def checkCompatibility(enumSourceA: String, enumSourceB: String)
    : CompatibilityResult[Enumeration#Value] = {
    import EnumValueSerializerUpgradeTest._

    val classLoader = compileAndLoadEnum(tempFolder.newFolder(), s"$enumName.scala", enumSourceA)

    val enum = instantiateEnum[Enumeration](classLoader, enumName)

    val enumValueSerializer = new EnumValueSerializer(enum)
    val snapshot = enumValueSerializer.snapshotConfiguration()

    val baos = new ByteArrayOutputStream()
    val output = new DataOutputViewStreamWrapper(baos)
    TypeSerializerSerializationUtil.writeSerializerConfigSnapshot(output, snapshot)

    output.close()
    baos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val input=  new DataInputViewStreamWrapper(bais)

    val classLoader2 = compileAndLoadEnum(tempFolder.newFolder(), s"$enumName.scala", enumSourceB)

    val snapshot2 = TypeSerializerSerializationUtil.readSerializerConfigSnapshot(
      input,
      classLoader2)
    val enum2 = instantiateEnum[Enumeration](classLoader2, enumName)

    val enumValueSerializer2 = new EnumValueSerializer(enum2)
    enumValueSerializer2.ensureCompatibility(snapshot2)
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

    val classLoader = Thread.currentThread().getContextClassLoader

    val urls = classLoader match {
      case urlClassLoader: URLClassLoader =>
        urlClassLoader.getURLs
      case x => throw new IllegalStateException(s"Not possible to extract URLs " +
        s"from class loader $x.")
    }

    settings.classpath.value = urls.map(_.toString).mkString(java.io.File.pathSeparator)
    settings.outdir.value = file.getParent

    val reporter = new ConsoleReporter(settings)
    val global = new Global(settings, reporter)
    val run = new global.Run

    run.compile(List(file.getAbsolutePath))

    reporter.printSummary()
  }
}

