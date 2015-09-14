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
package org.apache.flink.hcatalog.scala.test


import java.io.{File, FileWriter, IOException}

import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.hcatalog.scala.HCatOutputFormat
import org.apache.flink.test.util.TestBaseUtils
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState
import org.junit.Assert._
import org.junit.{After, Before, Test}

class HCatOutputFormatITCase {
  private var dataDir: File = null
  private var warehouseDir: String = null
  private var driver: Driver = null
  private var hiveConf: HiveConf = null

  @Before
  @throws(classOf[Exception])
  def setup {
    dataDir = new File(System.getProperty("java.io.tmpdir") + File.separator +
      this.getClass.getCanonicalName + "-" + System.currentTimeMillis)
    hiveConf = new HiveConf
    warehouseDir = makePathASafeFileName(dataDir + File.separator + "warehouse")
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "")
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "")
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false")
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseDir)
    driver = new Driver(hiveConf)
    SessionState.start(new CliSessionState(hiveConf))
    if (!(new File(warehouseDir).mkdirs)) {
      throw new RuntimeException("Could not create " + warehouseDir)
    }
  }

  @After
  @throws(classOf[IOException])
  def teardown {
    if (dataDir != null) {
      FileUtils.deleteDirectory(dataDir)
    }
  }

  @Test
  @throws(classOf[Exception])
  def testWriteComplexTypePartition() {
    val createTable: String = "CREATE TABLE test_table(" +
      "c1 array<string>,\n" +
      "c2 map<int,string>,\n" +
      "c3 struct<name:string,score:int>,\n" +
      "c4 string,\n" +
      "c5 float)\n" +
      "partitioned by (c0 string)\n" +
      "row format delimited " +
      "fields terminated by '\t' " +
      "COLLECTION ITEMS TERMINATED BY '/' " +
      "MAP KEYS TERMINATED BY ':' " +
      "STORED AS TEXTFILE"
    driver.run("drop table test_table")
    val retCode1: Int = driver.run(createTable).getResponseCode
    assertTrue("Table created", retCode1 == 0)
    val partitionValues: Map[String, String] = Map("c0" -> "part0")
    val opf: HCatOutputFormat[(List[String], Map[Int, String], List[Any], String, Float)] =
      new HCatOutputFormat(null, "test_table", partitionValues, hiveConf)
    val t1 = (List[String]("a1","b1","c1"), Map[Int, String](1 -> "v11", 2 -> "v12"),
      List[Any]("d", 1), "e1", 0.1f)
    val t2 = (List[String]("a2","b2","c2"), Map[Int, String](1 -> "v21", 2 -> "v22"),
      List[Any]("d", 2), "e2", 0.2f)
    val l: List[(List[String], Map[Int, String], List[Any], String, Float)]=
      List(t1, t2)
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val d: DataSet[(List[String], Map[Int, String], List[Any], String, Float)] =
      env.fromCollection(l)
    d.output(opf)
    env.execute()
    val outputFileName: String = makePathASafeFileName("file://" + warehouseDir +
      File.separator + "test_table/c0=part0/part-m-00001")
    val expected = "a1/b1/c1\t1:v11/2:v12\td/1\te1\t0.1\n" +
      "a2/b2/c2\t1:v21/2:v22\td/2\te2\t0.2"
    TestBaseUtils.compareResultsByLinesInMemory(expected, outputFileName)
  }

  @Test
  @throws(classOf[Exception])
  def testTypeInfoCheck() {
    val createTable: String = "CREATE TABLE test_table(" +
      "c1 array<string>,\n" +
      "c2 map<int,string>,\n" +
      "c3 struct<name:string,score:int>,\n" +
      "c4 string,\n" +
      "c5 float)\n" +
      "partitioned by (c0 string)\n" +
      "row format delimited " +
      "fields terminated by '\t' " +
      "COLLECTION ITEMS TERMINATED BY '/' " +
      "MAP KEYS TERMINATED BY ':' " +
      "STORED AS TEXTFILE"
    driver.run("drop table test_table")
    val retCode1: Int = driver.run(createTable).getResponseCode
    assertTrue("Table created", retCode1 == 0)
    val partitionValues: Map[String, String] = Map("c0" -> "part0")
    //wrong type, should throw exception
    try {
      val opf = new HCatOutputFormat[(List[String], Map[Int, String], List[Any], Float, Float)] (
        null, "test_table", partitionValues, hiveConf)
    } catch {
      case ioe: IOException => //succeeded!
      case _ => fail("expecting IOException!")
    }
  }

  @throws(classOf[IOException])
  def createTestDataFile(filename: String, lines: Array[String]) = {
    var writer: FileWriter = null
    try {
      val file: File = new File(filename)
      file.deleteOnExit()
      writer = new FileWriter(file)
      for (line <- lines) {
        writer.write(line + "\n")
      }
    } finally {
      if (writer != null) {
        writer.close()
      }
    }
  }

  def makePathASafeFileName(filePath: String): String = {
    new File(filePath).getPath.replaceAll("\\\\", "/")
  }
}
