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

package org.apache.flink.table.sources

import java.io.File

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableImpl
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.runtime.utils.TableUtil
import org.apache.flink.types.Row
import org.junit.{Assert, Test}

class CsvTableSourceITCase {

  private val cars_csv =
    """2012,"Tesla","S","No comment"
      |1997,Ford,E350,"Go get one now they are going fast"
      |2015,Chevy,Volt,""
      |""".stripMargin

  private val cars_csv_null =
    """2012,"Tesla","S","No comment"
      |,Ford,,"fast"
      |2015,Chevy,Volt,""
      |""".stripMargin

  @Test
  def testLookupJoinCsv(): Unit = {
    val env = StreamExecutionEnvironment
      .createLocalEnvironment(Runtime.getRuntime.availableProcessors, new Configuration())
    val tEnv = BatchTableEnvironment.create(env)

    val csvSource = CsvTableSource.builder()
      .path(createTmpCsvFile(cars_csv_null))
      .field("yr", BasicTypeInfo.INT_TYPE_INFO)
      .field("make", BasicTypeInfo.STRING_TYPE_INFO)
      .field("model", BasicTypeInfo.STRING_TYPE_INFO)
      .field("comment", BasicTypeInfo.STRING_TYPE_INFO)
      .build()
    tEnv.registerTableSource("cars", csvSource)

    val csvTemporal = CsvTableSource.builder()
      .path(createTmpCsvFile(cars_csv))
      .field("yr", BasicTypeInfo.INT_TYPE_INFO)
      .field("make", BasicTypeInfo.STRING_TYPE_INFO)
      .field("model", BasicTypeInfo.STRING_TYPE_INFO)
      .field("comment", BasicTypeInfo.STRING_TYPE_INFO)
      .build()
    tEnv.registerTableSource("carsTemporal", csvTemporal)

    val sql =
      """
        |SELECT C.yr, C.make, C.model, C.comment, T.yr, T.make, T.model, T.comment
        |FROM (SELECT yr, make, model, comment, PROCTIME() as proctime FROM cars) C
        |JOIN carsTemporal FOR SYSTEM_TIME AS OF C.proctime AS T
        |ON C.make = T.make
      """.stripMargin

    val results: Seq[Row] = TableUtil.collect(tEnv.sqlQuery(sql).asInstanceOf[TableImpl])

    val expected = List(
      "2012,\"Tesla\",\"S\",\"No comment\",2012,\"Tesla\",\"S\",\"No comment\"",
      "null,Ford,,\"fast\",1997,Ford,E350,\"Go get one now they are going fast\"",
      "2015,Chevy,Volt,\"\",2015,Chevy,Volt,\"\"")
    Assert.assertEquals(results.map(_.toString).sorted, expected.sorted)
  }

  def createTmpCsvFile(content: String): String = {
    val file = File.createTempFile("csvTest", ".csv")
    file.deleteOnExit()
    val filePath = file.getAbsolutePath

    import java.nio.file._
    Files.write(Paths.get(filePath), content.getBytes("UTF-8"))

    filePath
  }
}
