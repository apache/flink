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

package org.apache.flink.table.runtime.utils

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.catalog._
import org.apache.flink.table.descriptors.{OldCsv, FileSystem, Schema}
import org.apache.flink.table.sources.{BatchTableSource, CsvTableSource}

object CommonTestData {

  def getCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )

    val tempFilePath = writeToTempFile(csvRecords.mkString("$"), "csv-test", "tmp")
    CsvTableSource.builder()
      .path(tempFilePath)
      .field("first",Types.STRING)
      .field("id",Types.INT)
      .field("score",Types.DOUBLE)
      .field("last",Types.STRING)
      .fieldDelimiter("#")
      .lineDelimiter("$")
      .ignoreFirstLine()
      .commentPrefix("%")
      .build()
  }

  def getInMemoryTestCatalog(isStreaming: Boolean): ExternalCatalog = {
    val csvRecord1 = Seq(
      "1#1#Hi",
      "2#2#Hello",
      "3#2#Hello world"
    )
    val tempFilePath1 = writeToTempFile(csvRecord1.mkString("\n"), "csv-test1", "tmp")

    val connDesc1 = new FileSystem().path(tempFilePath1)
    val formatDesc1 = new OldCsv()
      .field("a", Types.INT)
      .field("b", Types.LONG)
      .field("c", Types.STRING)
      .fieldDelimiter("#")
    val schemaDesc1 = new Schema()
      .field("a", Types.INT)
      .field("b", Types.LONG)
      .field("c", Types.STRING)
    val externalTableBuilder1 = new ExternalCatalogTableBuilder(connDesc1)
      .withFormat(formatDesc1)
      .withSchema(schemaDesc1)

    if (isStreaming) {
      externalTableBuilder1.supportsStreaming().inAppendMode()
    } else {
      externalTableBuilder1.supportsBatch()
    }

    val csvRecord2 = Seq(
      "1#1#0#Hallo#1",
      "2#2#1#Hallo Welt#2",
      "2#3#2#Hallo Welt wie#1",
      "3#4#3#Hallo Welt wie gehts?#2",
      "3#5#4#ABC#2",
      "3#6#5#BCD#3",
      "4#7#6#CDE#2",
      "4#8#7#DEF#1",
      "4#9#8#EFG#1",
      "4#10#9#FGH#2",
      "5#11#10#GHI#1",
      "5#12#11#HIJ#3",
      "5#13#12#IJK#3",
      "5#14#13#JKL#2",
      "5#15#14#KLM#2"
    )
    val tempFilePath2 = writeToTempFile(csvRecord2.mkString("\n"), "csv-test2", "tmp")

    val connDesc2 = new FileSystem().path(tempFilePath2)
    val formatDesc2 = new OldCsv()
      .field("d", Types.INT)
      .field("e", Types.LONG)
      .field("f", Types.INT)
      .field("g", Types.STRING)
      .field("h", Types.LONG)
      .fieldDelimiter("#")
    val schemaDesc2 = new Schema()
      .field("d", Types.INT)
      .field("e", Types.LONG)
      .field("f", Types.INT)
      .field("g", Types.STRING)
      .field("h", Types.LONG)
    val externalTableBuilder2 = new ExternalCatalogTableBuilder(connDesc2)
      .withFormat(formatDesc2)
      .withSchema(schemaDesc2)

    if (isStreaming) {
      externalTableBuilder2.supportsStreaming().inAppendMode()
    } else {
      externalTableBuilder2.supportsBatch()
    }

    val tempFilePath3 = writeToTempFile("", "csv-test3", "tmp")
    val connDesc3 = new FileSystem().path(tempFilePath3)
    val formatDesc3 = new OldCsv()
      .field("x", Types.INT)
      .field("y", Types.LONG)
      .field("z", Types.STRING)
      .fieldDelimiter("#")
    val schemaDesc3 = new Schema()
      .field("x", Types.INT)
      .field("y", Types.LONG)
      .field("z", Types.STRING)
    val externalTableBuilder3 = new ExternalCatalogTableBuilder(connDesc3)
      .withFormat(formatDesc3)
      .withSchema(schemaDesc3)

    if (isStreaming) {
      externalTableBuilder3.supportsStreaming().inAppendMode()
    } else {
      externalTableBuilder3.supportsBatch()
    }

    val catalog = new InMemoryExternalCatalog("test")
    val db1 = new InMemoryExternalCatalog("db1")
    val db2 = new InMemoryExternalCatalog("db2")
    val db3 = new InMemoryExternalCatalog("db3")
    catalog.createSubCatalog("db1", db1, ignoreIfExists = false)
    catalog.createSubCatalog("db2", db2, ignoreIfExists = false)
    catalog.createSubCatalog("db3", db3, ignoreIfExists = false)

    // Register the table with both catalogs
    catalog.createTable("tb1", externalTableBuilder1.asTableSource(), ignoreIfExists = false)
    catalog.createTable("tb3", externalTableBuilder3.asTableSink(), ignoreIfExists = false)
    db1.createTable("tb1", externalTableBuilder1.asTableSource(), ignoreIfExists = false)
    db2.createTable("tb2", externalTableBuilder2.asTableSource(), ignoreIfExists = false)
    db3.createTable("tb3", externalTableBuilder3.asTableSink(), ignoreIfExists = false)
    catalog
  }

  private def writeToTempFile(
      contents: String,
      filePrefix: String,
      fileSuffix: String,
      charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }

  def getNestedTableSource: BatchTableSource[Person] = {
    new BatchTableSource[Person] {
      override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Person] = {
        val executionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        executionEnvironment.fromCollection(
          util.Arrays.asList(
            new Person("Mike", "Smith", new Address("5th Ave", "New-York")),
            new Person("Sally", "Miller", new Address("Potsdamer Platz", "Berlin")),
            new Person("Bob", "Taylor", new Address("Pearse Street", "Dublin"))),
          getReturnType
        )
      }

      override def getReturnType: TypeInformation[Person] = {
        TypeExtractor.getForClass(classOf[Person])
      }

      override def getTableSchema: TableSchema = TableSchema.fromTypeInfo(getReturnType)
    }
  }

  class Person(var firstName: String, var lastName: String, var address: Address) {
    def this() {
      this(null, null, null)
    }
  }

  class Address(var street: String, var city: String) {
    def this() {
      this(null, null)
    }
  }

  class NonPojo {
    val x = new java.util.HashMap[String, String]()

    override def toString: String = x.toString

    override def hashCode(): Int = super.hashCode()

    override def equals(obj: scala.Any): Boolean = super.equals(obj)
  }
}
