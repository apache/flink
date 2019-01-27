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

package org.apache.flink.table.sources.orc

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.types.TypeConverters
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.sinks.orc.OrcTableSink
import org.apache.flink.test.util.TestBaseUtils

import org.junit.{Assert, Test}

import java.nio.file.Files

import scala.collection.JavaConversions._

class OrcTableSinkITCase extends BatchTestBase {

  @Test
  def testOrcTableSinkOverwrite(): Unit = {
    // write
    val orcTable1 = CommonOrcTestData.getOrcVectorizedColumnRowTableSource(false)
    tEnv.registerTableSource("orcTable1", orcTable1)
    val tempFile = Files.createTempFile("orc-sink", "test")
    tempFile.toFile.deleteOnExit()

    try {
      tEnv.sqlQuery("SELECT id, `first`, `last`, score FROM orcTable1").writeToSink(
        new OrcTableSink(tempFile.toFile.getAbsolutePath))
      Assert.fail("runtime exception expected");
    } catch {
      case _: RuntimeException => None
      case _ => Assert.fail("runtime exception expected");
    }

    tEnv.sqlQuery("SELECT id, `first`, `last`, score FROM orcTable1").writeToSink(
      new OrcTableSink(tempFile.toFile.getAbsolutePath, Some(WriteMode.OVERWRITE)))
    tEnv.execute()
  }

  @Test
  def testOrcTableSink(): Unit = {
    // write
    val orcTable1 = CommonOrcTestData.getOrcVectorizedColumnRowTableSource(false)
    tEnv.registerTableSource("orcTable1", orcTable1)
    val tempFile = Files.createTempDirectory("parquet-sink")
    tEnv.sqlQuery("SELECT id, `first`, `last`, score FROM orcTable1").writeToSink(
      new OrcTableSink(tempFile.toFile.getAbsolutePath))
    tEnv.execute()
    tempFile.toFile.deleteOnExit()

    // read
    val names = Array("id", "first", "last", "score")
    val types: Array[TypeInformation[_]] = Array(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO
    )
    val orcTable2 = new OrcVectorizedColumnRowTableSource(
      new Path(tempFile.toFile.getAbsolutePath),
      types.map(TypeConverters.createInternalTypeFromTypeInfo(_)),
      names,
      true
    )
    tEnv.registerTableSource("orcTable2", orcTable2)
    val results = tEnv.sqlQuery("SELECT * FROM orcTable2").collect()
    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89",
      "4,Peter,Smith,0.12",
      "5,Liz,Williams,34.5",
      "6,Sally,Miller,6.78",
      "7,Alice,Smith,90.1",
      "8,Kelly,Williams,2.34").mkString("\n")

    TestBaseUtils.compareResultAsText(results, expected)
  }
}
