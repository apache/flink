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
package org.apache.flink.api.scala.io

import org.apache.flink.api.scala.operators.ScalaCsvInputFormat
import org.junit.Assert._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import java.io.File
import java.io.FileOutputStream
import java.io.FileWriter
import java.io.OutputStreamWriter
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.junit.Test
import org.apache.flink.api.scala._

class CsvInputFormatTest {

  private final val PATH: Path = new Path("an/ignored/file/")
  private final val FIRST_PART: String = "That is the first part"
  private final val SECOND_PART: String = "That is the second part"



  @Test
  def ignoreSingleCharPrefixComments():Unit = {
    try {
      val fileContent = "#description of the data\n" +
                        "#successive commented line\n" +
                        "this is|1|2.0|\n" +
                        "a test|3|4.0|\n" +
                        "#next|5|6.0|\n"
      val split = createTempFile(fileContent)
      val format = new ScalaCsvInputFormat[(String, Integer, Double)](
        PATH, createTypeInformation[(String, Integer, Double)])
      format.setDelimiter("\n")
      format.setFieldDelimiter('|')
      format.setCommentPrefix("#")
      val parameters = new Configuration
      format.configure(parameters)
      format.open(split)
      var result: (String, Integer, Double) = null
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("this is", result._1)
      assertEquals(new Integer(1), result._2)
      assertEquals(2.0, result._3, 0.0001)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("a test", result._1)
      assertEquals(new Integer(3), result._2)
      assertEquals(4.0, result._3, 0.0001)
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
      }
    }
  }

  @Test
  def ignoreMultiCharPrefixComments():Unit = {
    try {
      val fileContent = "//description of the data\n" +
                        "//successive commented line\n" +
                        "this is|1|2.0|\n" +
                        "a test|3|4.0|\n" +
                        "//next|5|6.0|\n"
      val split = createTempFile(fileContent)
      val format = new ScalaCsvInputFormat[(String, Integer, Double)](
        PATH, createTypeInformation[(String, Integer, Double)])
      format.setDelimiter("\n")
      format.setFieldDelimiter('|')
      format.setCommentPrefix("//")
      val parameters = new Configuration
      format.configure(parameters)
      format.open(split)
      var result: (String, Integer, Double) = null
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("this is", result._1)
      assertEquals(new Integer(1), result._2)
      assertEquals(2.0, result._3, 0.0001)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("a test", result._1)
      assertEquals(new Integer(3), result._2)
      assertEquals(4.0, result._3, 0.0001)
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
      }
    }
  }

  @Test
  def readStringFields():Unit = {
    try {
      val fileContent = "abc|def|ghijk\nabc||hhg\n|||"
      val split = createTempFile(fileContent)
      val format = new ScalaCsvInputFormat[(String, String, String)](
        PATH, createTypeInformation[(String, String, String)])
      format.setDelimiter("\n")
      format.setFieldDelimiter('|')
      val parameters = new Configuration
      format.configure(parameters)
      format.open(split)
      var result: (String, String, String) = null
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("abc", result._1)
      assertEquals("def", result._2)
      assertEquals("ghijk", result._3)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("abc", result._1)
      assertEquals("", result._2)
      assertEquals("hhg", result._3)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("", result._1)
      assertEquals("", result._2)
      assertEquals("", result._3)
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
      }
    }
  }

  @Test
  def readStringFieldsWithTrailingDelimiters(): Unit = {
    try {
      val fileContent = "abc|def|ghijk\nabc||hhg\n|||\n"
      val split = createTempFile(fileContent)
      val format = new ScalaCsvInputFormat[(String, String, String)](
        PATH, createTypeInformation[(String, String, String)])
      format.setDelimiter("\n")
      format.setFieldDelimiter('|')
      val parameters = new Configuration
      format.configure(parameters)
      format.open(split)
      var result: (String, String, String) = null
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("abc", result._1)
      assertEquals("def", result._2)
      assertEquals("ghijk", result._3)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("abc", result._1)
      assertEquals("", result._2)
      assertEquals("hhg", result._3)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("", result._1)
      assertEquals("", result._2)
      assertEquals("", result._3)
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
    }
    catch {
      case ex: Exception =>
        ex.printStackTrace()
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
    }
  }

  @Test
  def testIntegerFields(): Unit = {
    try {
      val fileContent = "111|222|333|444|555\n666|777|888|999|000|\n"
      val split = createTempFile(fileContent)
      val format = new ScalaCsvInputFormat[(Int, Int, Int, Int, Int)](
        PATH, createTypeInformation[(Int, Int, Int, Int, Int)])
      format.setFieldDelimiter('|')
      format.configure(new Configuration)
      format.open(split)
      var result: (Int, Int, Int, Int, Int) = null
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals(Integer.valueOf(111), result._1)
      assertEquals(Integer.valueOf(222), result._2)
      assertEquals(Integer.valueOf(333), result._3)
      assertEquals(Integer.valueOf(444), result._4)
      assertEquals(Integer.valueOf(555), result._5)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals(Integer.valueOf(666), result._1)
      assertEquals(Integer.valueOf(777), result._2)
      assertEquals(Integer.valueOf(888), result._3)
      assertEquals(Integer.valueOf(999), result._4)
      assertEquals(Integer.valueOf(000), result._5)
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
    }
    catch {
      case ex: Exception =>
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
    }
  }

  @Test
  def testReadFirstN(): Unit = {
    try {
      val fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n"
      val split = createTempFile(fileContent)
      val format = new ScalaCsvInputFormat[(Int, Int)](PATH, createTypeInformation[(Int, Int)])
      format.setFieldDelimiter('|')
      format.configure(new Configuration)
      format.open(split)
      var result: (Int, Int) = null
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals(Integer.valueOf(111), result._1)
      assertEquals(Integer.valueOf(222), result._2)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals(Integer.valueOf(666), result._1)
      assertEquals(Integer.valueOf(777), result._2)
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
    }
    catch {
      case ex: Exception =>
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
    }
  }

  @Test
  def testReadSparseWithPositionSetter(): Unit = {
    try {
      val fileContent: String = "111|222|333|444|555|666|777|888|999|000|\n000|999|888|777|666" +
        "|555|444|333|222|111|"
      val split = createTempFile(fileContent)
      val format = new ScalaCsvInputFormat[(Int, Int, Int)](
        PATH,
        createTypeInformation[(Int, Int, Int)])
      format.setFieldDelimiter('|')
      format.setFields(Array(0, 3, 7), Array(classOf[Integer], classOf[Integer], classOf[Integer]))
      format.configure(new Configuration)
      format.open(split)
      var result: (Int, Int, Int) = null
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals(Integer.valueOf(111), result._1)
      assertEquals(Integer.valueOf(444), result._2)
      assertEquals(Integer.valueOf(888), result._3)
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals(Integer.valueOf(000), result._1)
      assertEquals(Integer.valueOf(777), result._2)
      assertEquals(Integer.valueOf(333), result._3)
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
    }
    catch {
      case ex: Exception =>
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
    }
  }

  @Test
  def testReadSparseWithShuffledPositions(): Unit = {
    try {
      val format = new ScalaCsvInputFormat[(Int, Int, Int)](
        PATH,
        createTypeInformation[(Int, Int, Int)])
      format.setFieldDelimiter('|')
      try {
        format.setFields(Array(8, 1, 3), Array(classOf[Integer],classOf[Integer],classOf[Integer]))
        fail("Input sequence should have been rejected.")
      }
      catch {
        case e: IllegalArgumentException => // ignore
      }
    }
    catch {
      case ex: Exception =>
        fail("Test failed due to a " + ex.getClass.getName + ": " + ex.getMessage)
    }
  }

  private def createTempFile(content: String): FileInputSplit = {
    val tempFile = File.createTempFile("test_contents", "tmp")
    tempFile.deleteOnExit()
    val wrt = new FileWriter(tempFile)
    wrt.write(content)
    wrt.close()
    new FileInputSplit(0, new Path(tempFile.toURI.toString), 0,
      tempFile.length,Array[String]("localhost"))
  }

  @Test
  def testWindowsLineEndRemoval(): Unit = {
    this.testRemovingTrailingCR("\n", "\n")
    this.testRemovingTrailingCR("\r\n", "\r\n")
    this.testRemovingTrailingCR("\r\n", "\n")
  }

  private def testRemovingTrailingCR(lineBreakerInFile: String, lineBreakerSetup: String) {
    var tempFile: File = null
    val fileContent = FIRST_PART + lineBreakerInFile + SECOND_PART + lineBreakerInFile
    try {
      tempFile = File.createTempFile("CsvInputFormatTest", "tmp")
      tempFile.deleteOnExit()
      tempFile.setWritable(true)
      val wrt = new OutputStreamWriter(new FileOutputStream(tempFile))
      wrt.write(fileContent)
      wrt.close()
      val inputFormat = new ScalaCsvInputFormat[Tuple1[String]](new Path(tempFile.toURI.toString),
        createTypeInformation[Tuple1[String]])
      val parameters = new Configuration
      inputFormat.configure(parameters)
      inputFormat.setDelimiter(lineBreakerSetup)
      val splits = inputFormat.createInputSplits(1)
      inputFormat.open(splits(0))
      var result = inputFormat.nextRecord(null)
      assertNotNull("Expecting to not return null", result)
      assertEquals(FIRST_PART, result._1)
      result = inputFormat.nextRecord(result)
      assertNotNull("Expecting to not return null", result)
      assertEquals(SECOND_PART, result._1)
    }
    catch {
      case t: Throwable =>
        System.err.println("test failed with exception: " + t.getMessage)
        t.printStackTrace(System.err)
        fail("Test erroneous")
    }
  }
}

