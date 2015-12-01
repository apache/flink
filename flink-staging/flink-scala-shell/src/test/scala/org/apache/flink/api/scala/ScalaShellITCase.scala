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
import java.util.concurrent.TimeUnit

import org.apache.flink.test.util.{ForkableFlinkMiniCluster, TestBaseUtils}
import org.apache.flink.util.TestLogger
import org.junit.{AfterClass, BeforeClass, Test, Assert}

import scala.concurrent.duration.FiniteDuration
import scala.tools.nsc.Settings

class ScalaShellITCase extends TestLogger {

  import ScalaShellITCase._

  /** Prevent re-creation of environment */
  @Test
  def testPreventRecreation(): Unit = {

    val input: String =
      """
        val env = ExecutionEnvironment.getExecutionEnvironment
      """.stripMargin

    val output: String = processInShell(input)

    Assert.assertTrue(output.contains(
      "UnsupportedOperationException: Execution Environment is already " +
      "defined for this shell"))
  }

  /** Iteration test with iterative Pi example */
  @Test
  def testIterativePI(): Unit = {

    val input: String =
      """
        val initial = env.fromElements(0)
        val count = initial.iterate(10000) { iterationInput: DataSet[Int] =>
          val result = iterationInput.map { i =>
            val x = Math.random()
            val y = Math.random()
            i + (if (x * x + y * y < 1) 1 else 0)
          }
          result
        }
        val result = count map { c => c / 10000.0 * 4 }
        result.collect()
      """.stripMargin

    val output: String = processInShell(input)

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))
  }

  /** WordCount in Shell */
  @Test
  def testWordCount(): Unit = {
    val input =
      """
        val text = env.fromElements("To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,")
        val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
        val result = counts.print()
      """.stripMargin

    val output = processInShell(input)

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))

    // some of the words that should be included
    Assert.assertTrue(output.contains("(a,1)"))
    Assert.assertTrue(output.contains("(whether,1)"))
    Assert.assertTrue(output.contains("(to,4)"))
    Assert.assertTrue(output.contains("(arrows,1)"))
  }

  /** Sum 1..10, should be 55 */
  @Test
  def testSum: Unit = {
    val input =
      """
        val input: DataSet[Int] = env.fromElements(0,1,2,3,4,5,6,7,8,9,10)
        val reduced = input.reduce(_+_)
        reduced.print
      """.stripMargin

    val output = processInShell(input)

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))

    Assert.assertTrue(output.contains("55"))
  }

  /** WordCount in Shell with custom case class */
  @Test
  def testWordCountWithCustomCaseClass: Unit = {
    val input =
      """
      case class WC(word: String, count: Int)
      val wordCounts = env.fromElements(
        new WC("hello", 1),
        new WC("world", 2),
        new WC("world", 8))
      val reduced = wordCounts.groupBy(0).sum(1)
      reduced.print()
      """.stripMargin

    val output = processInShell(input)

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))

    Assert.assertTrue(output.contains("WC(hello,1)"))
    Assert.assertTrue(output.contains("WC(world,10)"))
  }

  /** Submit external library */
  @Test
  def testSubmissionOfExternalLibrary: Unit = {
    val input =
      """
        import org.apache.flink.ml.math._
        val denseVectors = env.fromElements(DenseVector(1.0, 2.0, 3.0))
        denseVectors.print()
      """.stripMargin

    // find jar file that contains the ml code
    var externalJar = ""
    val folder = new File("../flink-ml/target/")
    val listOfFiles = folder.listFiles()

    for (i <- listOfFiles.indices) {
      val filename: String = listOfFiles(i).getName
      if (!filename.contains("test") && !filename.contains("original") && filename.contains(
        ".jar")) {
        externalJar = listOfFiles(i).getAbsolutePath
      }
    }

    assert(externalJar != "")

    val output: String = processInShell(input, Option(externalJar))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))

    Assert.assertTrue(output.contains("\nDenseVector(1.0, 2.0, 3.0)"))
  }


  /**
   * tests flink shell startup with remote cluster (starts cluster internally)
   */
  @Test
  def testRemoteCluster: Unit = {

    val input: String =
      """
        |import org.apache.flink.api.common.functions.RichMapFunction
        |import org.apache.flink.api.java.io.PrintingOutputFormat
        |import org.apache.flink.api.common.accumulators.IntCounter
        |import org.apache.flink.configuration.Configuration
        |
        |val els = env.fromElements("foobar","barfoo")
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
        |val executionResult = env.execute("Test Job")
        |System.out.println("IntCounter: " + executionResult.getIntCounterResult("intCounter"))
        |
        |:q
      """.stripMargin

    val in: BufferedReader = new BufferedReader(
      new StringReader(
        input + "\n"))
    val out: StringWriter = new StringWriter

    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))

    val (c, args) = cluster match{
      case Some(cl) =>
        val arg = Array("remote",
          cl.hostname,
          Integer.toString(cl.getLeaderRPCPort))
        (cl, arg)
      case None =>
        throw new AssertionError("Cluster creation failed.")
    }

    //start scala shell with initialized
    // buffered reader for testing
    FlinkShell.bufferedReader = Some(in)
    FlinkShell.main(args)
    baos.flush()

    val output: String = baos.toString
    System.setOut(oldOut)

    Assert.assertTrue(output.contains("IntCounter: 2"))
    Assert.assertTrue(output.contains("foobar"))
    Assert.assertTrue(output.contains("barfoo"))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("Error"))
    Assert.assertFalse(output.contains("ERROR"))
    Assert.assertFalse(output.contains("Exception"))
  }
}

object ScalaShellITCase {
  var cluster: Option[ForkableFlinkMiniCluster] = None
  val parallelism = 4

  @BeforeClass
  def beforeAll(): Unit = {
    val cl = TestBaseUtils.startCluster(
      1,
      parallelism,
      false,
      false,
      false)

    cluster = Some(cl)
  }

  @AfterClass
  def afterAll(): Unit = {
    // The Scala interpreter somehow changes the class loader. Therfore, we have to reset it
    Thread.currentThread().setContextClassLoader(classOf[ScalaShellITCase].getClassLoader)
    cluster.foreach(c => TestBaseUtils.stopCluster(c, new FiniteDuration(1000, TimeUnit.SECONDS)))
  }

  /**
   * Run the input using a Scala Shell and return the output of the shell.
   * @param input commands to be processed in the shell
   * @return output of shell
   */
  def processInShell(input: String, externalJars: Option[String] = None): String = {
    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val baos = new ByteArrayOutputStream()

    val oldOut = System.out
    System.setOut(new PrintStream(baos))

    // new local cluster
    val host = "localhost"
    val port = cluster match {
      case Some(c) => c.getLeaderRPCPort
      case _ => throw new RuntimeException("Test cluster not initialized.")
    }

    val repl = externalJars match {
      case Some(ej) => new FlinkILoop(
        host, port,
        Option(Array(ej)),
        in, new PrintWriter(out))

      case None => new FlinkILoop(
        host, port,
        in, new PrintWriter(out))
    }

    repl.settings = new Settings()

    // enable this line to use scala in intellij
    repl.settings.usejavacp.value = true

    externalJars match {
      case Some(ej) => repl.settings.classpath.value = ej
      case None =>
    }

    repl.process(repl.settings)

    repl.closeInterpreter()

    System.setOut(oldOut)

    baos.flush()

    val stdout = baos.toString

    out.toString + stdout
  }
}
