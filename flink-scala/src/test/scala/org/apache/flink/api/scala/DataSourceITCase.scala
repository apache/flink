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
package org.apache.flink.api.scala

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.junit.Test

/**
  * Data source IT test case.
  */
class DataSourceITCase {

  @throws[Exception]
  @Test
  def testWithAndGetParameters(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ifConf: Configuration = new Configuration
    ifConf.setString("prepend", "test")
    val ds: DataSource[String] = env
      .createInput(new TestInputFormat(new Path("/tmp")))
      .withParameters(ifConf)
    val conf = ds.getParameters
    assertTrue(conf.containsKey("prepend"))
    assertEquals("test", conf.getString("prepend", null))
  }

  @Test
  def testGetSplitDataProperties(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val inputFormat = new Tuple2InputFormat()
    val ds: DataSource[(String, String)] = env.createInput(inputFormat)
    assertNotNull(ds.getSplitDataProperties())
    ds.getSplitDataProperties().splitsGroupedBy(0)
    val expectedGroupKeys = ds.getSplitDataProperties().getSplitGroupKeys
    assertEquals(1, expectedGroupKeys.size)
    assertEquals(0, expectedGroupKeys(0))
  }

  @Test
  def testGetInputFormat(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val inputFormat = new TestInputFormat(new Path("/tmp"))
    inputFormat.setBufferSize(Integer.MAX_VALUE)
    val ds: DataSource[String] = env.createInput(inputFormat)
    assertTrue(ds.getInputFormat.isInstanceOf[TestInputFormat])
    val expectedInputFormat = ds.getInputFormat.asInstanceOf[TestInputFormat]
    assertEquals(Integer.MAX_VALUE, expectedInputFormat.getBufferSize)
  }

  private class TestInputFormat(filePath: Path) extends TextInputFormat(filePath) {
    override def configure(parameters: Configuration): Unit = {
      super.configure(parameters)
      assertNotNull(parameters.getString("prepend", null))
      assertEquals("test", parameters.getString("prepend", null))
    }
  }

  private class Tuple2InputFormat extends FileInputFormat[(String, String)] {
    override def reachedEnd(): Boolean = false

    override def nextRecord(reuse: (String, String)): (String, String) = null
  }

}
