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

import java.io._

import org.apache.flink.configuration.{Configuration, CoreOptions}
import org.apache.flink.runtime.clusterframework.BootstrapTools
import org.apache.flink.util.TestLogger
import org.junit.{Assert, Rule, Test}
import org.junit.rules.TemporaryFolder

class ScalaShellLocalStartupITCase extends TestLogger {

  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  /**
   * tests flink shell with local setup through startup script in bin folder
   * for both streaming and batch
   */
  @Test
  def testLocalCluster: Unit = {
    val input: String =
      """
        |import org.apache.flink.api.common.functions.RichMapFunction
        |import org.apache.flink.api.java.io.PrintingOutputFormat
        |import org.apache.flink.api.common.accumulators.IntCounter
        |import org.apache.flink.configuration.Configuration
        |
        |val els = benv.fromElements("foobar","barfoo")
        |val mapped = els.map{
        | new RichMapFunction[String, String]() {
        |   var intCounter: IntCounter = _
        |   override def open(conf: Configuration): Unit = {
        |     intCounter = getRuntimeContext.getIntCounter("intCounter")
        |   }
        |
        |   def map(element: String): String = {
        |     intCounter.add(1)
        |     element
        |   }
        | }
        |}
        |mapped.output(new PrintingOutputFormat())
        |val executionResult = benv.execute("Test Job")
        |System.out.println("IntCounter: " + executionResult.getIntCounterResult("intCounter"))
        |
        |val elss = senv.fromElements("foobar","barfoo")
        |val mapped = elss.map{
        |     new RichMapFunction[String,String]() {
        |     def map(element:String): String = {
        |     element + "Streaming"
        |   }
        |  }
        |}
        |
        |mapped.print
        |senv.execute("awesome streaming process")
        |
        |:q
      """.stripMargin
    val in: BufferedReader = new BufferedReader(new StringReader(input + "\n"))
    val out: StringWriter = new StringWriter
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))

    val configuration = new Configuration()
    configuration.setString(CoreOptions.MODE, CoreOptions.LEGACY_MODE)

    val dir = temporaryFolder.newFolder()
    BootstrapTools.writeConfiguration(configuration, new File(dir, "flink-conf.yaml"))

    val args: Array[String] = Array("local", "--configDir", dir.getAbsolutePath)

    //start flink scala shell
    FlinkShell.bufferedReader = Some(in);
    FlinkShell.main(args)

    baos.flush()
    val output: String = baos.toString
    System.setOut(oldOut)

    Assert.assertTrue(output.contains("IntCounter: 2"))
    Assert.assertTrue(output.contains("foobar"))
    Assert.assertTrue(output.contains("barfoo"))

    Assert.assertTrue(output.contains("foobarStream"))
    Assert.assertTrue(output.contains("barfooStream"))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("Error"))
    Assert.assertFalse(output.contains("ERROR"))
    Assert.assertFalse(output.contains("Exception"))
  }
}
