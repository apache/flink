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

package org.apache.flink.api.table.runtime.io

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.io.ParseException
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.runtime.io.RowCsvInputFormatTest.{PATH, createTempFile, testRemovingTrailingCR}
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.types.parser.{FieldParser, StringParser}
import org.junit.Assert._
import org.junit.{Ignore, Test}

class RowCsvInputFormatTest {

  @Test
  def ignoreInvalidLines() {
    val fileContent =
      "#description of the data\n" +
        "header1|header2|header3|\n" +
        "this is|1|2.0|\n" +
        "//a comment\n" +
        "a test|3|4.0|\n" +
        "#next|5|6.0|\n"

    val split = createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, typeInfo, "\n", "|")
    format.setLenient(false)
    val parameters = new Configuration
    format.configure(parameters)
    format.open(split)

    var result = new Row(3)
    try {
      result = format.nextRecord(result)
      fail("Parse Exception was not thrown! (Row too short)")
    }
    catch {
      case ex: ParseException =>  // ok
    }

    try {
      result = format.nextRecord(result)
      fail("Parse Exception was not thrown! (Invalid int value)")
    }
    catch {
      case ex: ParseException => // ok
    }

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("this is", result.productElement(0))
    assertEquals(1, result.productElement(1))
    assertEquals(2.0, result.productElement(2))

    try {
      result = format.nextRecord(result)
      fail("Parse Exception was not thrown! (Row too short)")
    }
    catch {
      case ex: ParseException => // ok
    }

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("a test", result.productElement(0))
    assertEquals(3, result.productElement(1))
    assertEquals(4.0, result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("#next", result.productElement(0))
    assertEquals(5, result.productElement(1))
    assertEquals(6.0, result.productElement(2))

    result = format.nextRecord(result)
    assertNull(result)

    // re-open with lenient = true
    format.setLenient(true)
    format.configure(parameters)
    format.open(split)

    result = new Row(3)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("header1", result.productElement(0))
    assertNull(result.productElement(1))
    assertNull(result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("this is", result.productElement(0))
    assertEquals(1, result.productElement(1))
    assertEquals(2.0, result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("a test", result.productElement(0))
    assertEquals(3, result.productElement(1))
    assertEquals(4.0, result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("#next", result.productElement(0))
    assertEquals(5, result.productElement(1))
    assertEquals(6.0, result.productElement(2))
    result = format.nextRecord(result)
    assertNull(result)
  }

  @Test
  def ignoreSingleCharPrefixComments() {
    val fileContent =
      "#description of the data\n" +
        "#successive commented line\n" +
        "this is|1|2.0|\n" +
        "a test|3|4.0|\n" +
        "#next|5|6.0|\n"

    val split = createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, typeInfo, "\n", "|")
    format.setCommentPrefix("#")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(3)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("this is", result.productElement(0))
    assertEquals(1, result.productElement(1))
    assertEquals(2.0, result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("a test", result.productElement(0))
    assertEquals(3, result.productElement(1))
    assertEquals(4.0, result.productElement(2))

    result = format.nextRecord(result)
    assertNull(result)
  }

  @Test
  def ignoreMultiCharPrefixComments() {
    val fileContent =
      "//description of the data\n" +
        "//successive commented line\n" +
        "this is|1|2.0|\n" +
        "a test|3|4.0|\n" +
        "//next|5|6.0|\n"

    val split = createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, typeInfo, "\n", "|")
    format.setCommentPrefix("//")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(3)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("this is", result.productElement(0))
    assertEquals(1, result.productElement(1))
    assertEquals(2.0, result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("a test", result.productElement(0))
    assertEquals(3, result.productElement(1))
    assertEquals(4.0, result.productElement(2))
    result = format.nextRecord(result)
    assertNull(result)
  }

  @Test
  def readStringFields() {
    val fileContent = "abc|def|ghijk\nabc||hhg\n|||"
    
    val split = createTempFile(fileContent)
    
    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO))
    
    val format = new RowCsvInputFormat(PATH, typeInfo, "\n", "|")
    format.configure(new Configuration)
    format.open(split)
    
    var result = new Row(3)
    
    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("abc", result.productElement(0))
    assertEquals("def", result.productElement(1))
    assertEquals("ghijk", result.productElement(2))
    
    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("abc", result.productElement(0))
    assertEquals("", result.productElement(1))
    assertEquals("hhg", result.productElement(2))
    
    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("", result.productElement(0))
    assertEquals("", result.productElement(1))
    assertEquals("", result.productElement(2))
    
    result = format.nextRecord(result)
    assertNull(result)
    assertTrue(format.reachedEnd)
  }

  @Test def readMixedQuotedStringFields() {
      val fileContent = "@a|b|c@|def|@ghijk@\nabc||@|hhg@\n|||"
      
      val split = createTempFile(fileContent)
      
      val typeInfo = new RowTypeInfo(Seq(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO))
      
      val format = new RowCsvInputFormat(PATH, typeInfo, "\n", "|")
      format.configure(new Configuration)
      format.enableQuotedStringParsing('@')
      format.open(split)
    
      var result = new Row(3)
    
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("a|b|c", result.productElement(0))
      assertEquals("def", result.productElement(1))
      assertEquals("ghijk", result.productElement(2))
    
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("abc", result.productElement(0))
      assertEquals("", result.productElement(1))
      assertEquals("|hhg", result.productElement(2))
    
      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals("", result.productElement(0))
      assertEquals("", result.productElement(1))
      assertEquals("", result.productElement(2))
    
      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
  }

  @Test def readStringFieldsWithTrailingDelimiters() {
    val fileContent = "abc|-def|-ghijk\nabc|-|-hhg\n|-|-|-\n"

    val split = createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, typeInfo, "\n", "|")
    format.setFieldDelimiter("|-")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(3)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("abc", result.productElement(0))
    assertEquals("def", result.productElement(1))
    assertEquals("ghijk", result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("abc", result.productElement(0))
    assertEquals("", result.productElement(1))
    assertEquals("hhg", result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals("", result.productElement(0))
    assertEquals("", result.productElement(1))
    assertEquals("", result.productElement(2))

    result = format.nextRecord(result)
    assertNull(result)
    assertTrue(format.reachedEnd)
  }

  @Test
  def testIntegerFields() {
    val fileContent = "111|222|333|444|555\n666|777|888|999|000|\n"
    
    val split = createTempFile(fileContent)
    
    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO))
    
    val format = new RowCsvInputFormat(PATH, typeInfo, "\n", "|")

    format.setFieldDelimiter("|")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(5)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(111, result.productElement(0))
    assertEquals(222, result.productElement(1))
    assertEquals(333, result.productElement(2))
    assertEquals(444, result.productElement(3))
    assertEquals(555, result.productElement(4))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(666, result.productElement(0))
    assertEquals(777, result.productElement(1))
    assertEquals(888, result.productElement(2))
    assertEquals(999, result.productElement(3))
    assertEquals(0, result.productElement(4))

    result = format.nextRecord(result)
    assertNull(result)
    assertTrue(format.reachedEnd)
  }

  @Test
  def testEmptyFields() {
    val fileContent =
      "|0|0|0|0|0|\n" +
        "1||1|1|1|1|\n" +
        "2|2||2|2|2|\n" +
        "3|3|3||3|3|\n" +
        "4|4|4|4||4|\n" +
        "5|5|5|5|5||\n"

    val split = createTempFile(fileContent)

    // TODO: FLOAT_TYPE_INFO and DOUBLE_TYPE_INFO don't handle correctly null values
    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.SHORT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.BYTE_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, rowTypeInfo = typeInfo)
    format.setFieldDelimiter("|")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(6)
    val linesCnt = fileContent.split("\n").length

    var i = 0
    while (i < linesCnt) {
      result = format.nextRecord(result)
      assertNull(result.productElement(i))
      i += 1
    }
    
    // ensure no more rows
    assertNull(format.nextRecord(result))
    assertTrue(format.reachedEnd)
  }

  @Test
  def testDoubleFields() {
    val fileContent = "11.1|22.2|33.3|44.4|55.5\n66.6|77.7|88.8|99.9|00.0|\n"

    val split = createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, rowTypeInfo = typeInfo)
    format.setFieldDelimiter("|")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(5)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(11.1, result.productElement(0))
    assertEquals(22.2, result.productElement(1))
    assertEquals(33.3, result.productElement(2))
    assertEquals(44.4, result.productElement(3))
    assertEquals(55.5, result.productElement(4))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(66.6, result.productElement(0))
    assertEquals(77.7, result.productElement(1))
    assertEquals(88.8, result.productElement(2))
    assertEquals(99.9, result.productElement(3))
    assertEquals(0.0, result.productElement(4))

    result = format.nextRecord(result)
    assertNull(result)
    assertTrue(format.reachedEnd)
  }

  @Test
  def testReadFirstN() {
    val fileContent = "111|222|333|444|555|\n666|777|888|999|000|\n"

    val split = createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, rowTypeInfo = typeInfo)
    format.setFieldDelimiter("|")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(2)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(111, result.productElement(0))
    assertEquals(222, result.productElement(1))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(666, result.productElement(0))
    assertEquals(777, result.productElement(1))

    result = format.nextRecord(result)
    assertNull(result)
    assertTrue(format.reachedEnd)
  }

  @Test
  def testReadSparseWithNullFieldsForTypes() {
    val fileContent = "111|x|222|x|333|x|444|x|555|x|666|x|777|x|888|x|999|x|000|x|\n" +
      "000|x|999|x|888|x|777|x|666|x|555|x|444|x|333|x|222|x|111|x|"

    val split = createTempFile(fileContent)

    val typeInfo: RowTypeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO))

    val format = new RowCsvInputFormat(
      PATH,
      rowTypeInfo = typeInfo,
      includedFieldsMask = Array(true, false, false, true, false, false, false, true))
    format.setFieldDelimiter("|x|")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(3)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(111, result.productElement(0))
    assertEquals(444, result.productElement(1))
    assertEquals(888, result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(0, result.productElement(0))
    assertEquals(777, result.productElement(1))
    assertEquals(333, result.productElement(2))

    result = format.nextRecord(result)
    assertNull(result)
    assertTrue(format.reachedEnd)
  }

  @Test
  def testReadSparseWithPositionSetter() {
      val fileContent = "111|222|333|444|555|666|777|888|999|000|\n" +
        "000|999|888|777|666|555|444|333|222|111|"

      val split = createTempFile(fileContent)

      val typeInfo = new RowTypeInfo(Seq(
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO))

      val format = new RowCsvInputFormat(
        PATH,
        typeInfo,
        Array(0, 3, 7))
      format.setFieldDelimiter("|")
      format.configure(new Configuration)
      format.open(split)

      var result = new Row(3)
      result = format.nextRecord(result)

      assertNotNull(result)
      assertEquals(111, result.productElement(0))
      assertEquals(444, result.productElement(1))
      assertEquals(888, result.productElement(2))

      result = format.nextRecord(result)
      assertNotNull(result)
      assertEquals(0, result.productElement(0))
      assertEquals(777, result.productElement(1))
      assertEquals(333, result.productElement(2))

      result = format.nextRecord(result)
      assertNull(result)
      assertTrue(format.reachedEnd)
  }

  @Test
  def testReadSparseWithMask() {
    val fileContent = "111&&222&&333&&444&&555&&666&&777&&888&&999&&000&&\n" +
      "000&&999&&888&&777&&666&&555&&444&&333&&222&&111&&"

    val split = RowCsvInputFormatTest.createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO))

    val format = new RowCsvInputFormat(
      PATH,
      rowTypeInfo = typeInfo,
      includedFieldsMask = Array(true, false, false, true, false, false, false, true))
    format.setFieldDelimiter("&&")
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(3)

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(111, result.productElement(0))
    assertEquals(444, result.productElement(1))
    assertEquals(888, result.productElement(2))

    result = format.nextRecord(result)
    assertNotNull(result)
    assertEquals(0, result.productElement(0))
    assertEquals(777, result.productElement(1))
    assertEquals(333, result.productElement(2))

    result = format.nextRecord(result)
    assertNull(result)
    assertTrue(format.reachedEnd)
  }

  @Test
  def testParseStringErrors() {
    val stringParser = new StringParser
    stringParser.enableQuotedStringParsing('"'.toByte)

    val failures = Seq(
      ("\"string\" trailing", FieldParser.ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING),
      ("\"unterminated ", FieldParser.ParseErrorState.UNTERMINATED_QUOTED_STRING)
    )

    for (failure <- failures) {
      val result = stringParser.parseField(
        failure._1.getBytes,
        0,
        failure._1.length,
        Array[Byte]('|'),
        null)

      assertEquals(-1, result)
      assertEquals(failure._2, stringParser.getErrorState)
    }
  }

  // Test disabled because we do not support double-quote escaped quotes right now.
  @Test
  @Ignore
  def testParserCorrectness() {
    // RFC 4180 Compliance Test content
    // Taken from http://en.wikipedia.org/wiki/Comma-separated_values#Example
    val fileContent = "Year,Make,Model,Description,Price\n" +
      "1997,Ford,E350,\"ac, abs, moon\",3000.00\n" +
      "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n" +
      "1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\",4799.00\n" +
      "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n" +
      ",,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00"

    val split = createTempFile(fileContent)

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO))

    val format = new RowCsvInputFormat(PATH, typeInfo)
    format.setSkipFirstLineAsHeader(true)
    format.setFieldDelimiter(',')
    format.configure(new Configuration)
    format.open(split)

    var result = new Row(5)
    val r1: Row = new Row(5)
    r1.setField(0, 1997)
    r1.setField(1, "Ford")
    r1.setField(2, "E350")
    r1.setField(3, "ac, abs, moon")
    r1.setField(4, 3000.0)

    val r2: Row = new Row(5)
    r2.setField(0, 1999)
    r2.setField(1, "Chevy")
    r2.setField(2, "Venture \"Extended Edition\"")
    r2.setField(3, "")
    r2.setField(4, 4900.0)

    val r3: Row = new Row(5)
    r3.setField(0, 1996)
    r3.setField(1, "Jeep")
    r3.setField(2, "Grand Cherokee")
    r3.setField(3, "MUST SELL! air, moon roof, loaded")
    r3.setField(4, 4799.0)

    val r4: Row = new Row(5)
    r4.setField(0, 1999)
    r4.setField(1, "Chevy")
    r4.setField(2, "Venture \"Extended Edition, Very Large\"")
    r4.setField(3, "")
    r4.setField(4, 5000.0)

    val r5: Row = new Row(5)
    r5.setField(0, 0)
    r5.setField(1, "")
    r5.setField(2, "Venture \"Extended Edition\"")
    r5.setField(3, "")
    r5.setField(4, 4900.0)

    val expectedLines = Array(r1, r2, r3, r4, r5)
    for (expected <- expectedLines) {
      result = format.nextRecord(result)
      assertEquals(expected, result)
    }
    assertNull(format.nextRecord(result))
    assertTrue(format.reachedEnd)
  }

  @Test
  def testWindowsLineEndRemoval() {

    // check typical use case -- linux file is correct and it is set up to linux(\n)
    testRemovingTrailingCR("\n", "\n")

    // check typical windows case -- windows file endings and file has windows file endings set up
    testRemovingTrailingCR("\r\n", "\r\n")

    // check problematic case windows file -- windows file endings(\r\n)
    // but linux line endings (\n) set up
    testRemovingTrailingCR("\r\n", "\n")

    // check problematic case linux file -- linux file endings (\n)
    // but windows file endings set up (\r\n)
    // specific setup for windows line endings will expect \r\n because
    // it has to be set up and is not standard.
  }

  @Test
  def testQuotedStringParsingWithIncludeFields() {
    val fileContent = "\"20:41:52-1-3-2015\"|\"Re: Taskmanager memory error in Eclipse\"|" +
      "\"Blahblah <blah@blahblah.org>\"|\"blaaa|\"blubb\""
    val tempFile = File.createTempFile("CsvReaderQuotedString", "tmp")
    tempFile.deleteOnExit()
    tempFile.setWritable(true)

    val writer = new OutputStreamWriter(new FileOutputStream(tempFile))
    writer.write(fileContent)
    writer.close()

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO))

    val inputFormat = new RowCsvInputFormat(
      new Path(tempFile.toURI.toString),
      rowTypeInfo = typeInfo,
      includedFieldsMask = Array(true, false, true))
    inputFormat.enableQuotedStringParsing('"')
    inputFormat.setFieldDelimiter('|')
    inputFormat.setDelimiter('\n')
    inputFormat.configure(new Configuration)

    val splits = inputFormat.createInputSplits(1)
    inputFormat.open(splits(0))

    val record = inputFormat.nextRecord(new Row(2))
    assertEquals("20:41:52-1-3-2015", record.productElement(0))
    assertEquals("Blahblah <blah@blahblah.org>", record.productElement(1))
  }

  @Test
  def testQuotedStringParsingWithEscapedQuotes() {
    val fileContent = "\"\\\"Hello\\\" World\"|\"We are\\\" young\""
    val tempFile = File.createTempFile("CsvReaderQuotedString", "tmp")
    tempFile.deleteOnExit()
    tempFile.setWritable(true)

    val writer = new OutputStreamWriter(new FileOutputStream(tempFile))
    writer.write(fileContent)
    writer.close()

    val typeInfo = new RowTypeInfo(Seq(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO))

    val inputFormat = new RowCsvInputFormat(
      new Path(tempFile.toURI.toString),
      rowTypeInfo = typeInfo)
    inputFormat.enableQuotedStringParsing('"')
    inputFormat.setFieldDelimiter('|')
    inputFormat.setDelimiter('\n')
    inputFormat.configure(new Configuration)

    val splits = inputFormat.createInputSplits(1)
    inputFormat.open(splits(0))

    val record = inputFormat.nextRecord(new Row(2))
    assertEquals("\\\"Hello\\\" World", record.productElement(0))
    assertEquals("We are\\\" young", record.productElement(1))
  }
}

object RowCsvInputFormatTest {

  private val PATH = new Path("an/ignored/file/")

  // static variables for testing the removal of \r\n to \n
  private val FIRST_PART = "That is the first part"
  private val SECOND_PART = "That is the second part"

  private def createTempFile(content: String): FileInputSplit = {
      val tempFile = File.createTempFile("test_contents", "tmp")
      tempFile.deleteOnExit()
      val wrt = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)
      wrt.write(content)
      wrt.close()
      new FileInputSplit(
        0,
        new Path(tempFile.toURI.toString),
        0,
        tempFile.length,
        Array("localhost"))
  }

  private def testRemovingTrailingCR(lineBreakerInFile: String, lineBreakerSetup: String) {
    val fileContent = FIRST_PART + lineBreakerInFile + SECOND_PART + lineBreakerInFile

    // create input file
    val tempFile = File.createTempFile("CsvInputFormatTest", "tmp")
    tempFile.deleteOnExit()
    tempFile.setWritable(true)

    val wrt = new OutputStreamWriter(new FileOutputStream(tempFile))
    wrt.write(fileContent)
    wrt.close()

    val typeInfo = new RowTypeInfo(Seq(BasicTypeInfo.STRING_TYPE_INFO))

    val inputFormat = new RowCsvInputFormat(new Path(tempFile.toURI.toString), typeInfo)
    inputFormat.configure(new Configuration)
    inputFormat.setDelimiter(lineBreakerSetup)

    val splits = inputFormat.createInputSplits(1)
    inputFormat.open(splits(0))

    var result = inputFormat.nextRecord(new Row(1))
    assertNotNull("Expecting to not return null", result)
    assertEquals(FIRST_PART, result.productElement(0))

    result = inputFormat.nextRecord(result)
    assertNotNull("Expecting to not return null", result)
    assertEquals(SECOND_PART, result.productElement(0))
  }
}
