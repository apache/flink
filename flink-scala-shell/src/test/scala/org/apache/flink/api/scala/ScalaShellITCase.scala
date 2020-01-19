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

import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.configuration.{Configuration, DeploymentOptions, JobManagerOptions, RestOptions}
import org.apache.flink.runtime.clusterframework.BootstrapTools
import org.apache.flink.runtime.minicluster.MiniCluster
import org.apache.flink.runtime.testutils.MiniClusterResource
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.util.TestLogger
import org.junit._
import org.junit.rules.TemporaryFolder

import scala.tools.nsc.Settings

class ScalaShellITCase extends TestLogger {

  import ScalaShellITCase._

  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  @After
  def resetClassLoder(): Unit = {
    // The Scala interpreter changes current class loader to ScalaClassLoader in every execution
    // refer to [[ILoop.process()]]. So, we need reset it to original class loader after every Test.
    Thread.currentThread().setContextClassLoader(classOf[ScalaShellITCase].getClassLoader)
  }

  /** Prevent re-creation of environment */
  @Test
  def testPreventRecreationBatch(): Unit = {

    val input: String =
      """
        val benv = ExecutionEnvironment.getExecutionEnvironment
      """.stripMargin

    val output: String = processInShell(input)

    Assert.assertTrue(output.contains(
      "UnsupportedOperationException: Execution Environment is already " +
      "defined for this shell"))
  }

  /** Prevent re-creation of environment */
  @Test
  def testPreventRecreationStreaming(): Unit = {

    val input: String =
      """
        import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
      """.stripMargin

    val output: String = processInShell(input)

    Assert.assertTrue(output.contains(
      "UnsupportedOperationException: Execution Environment is already " +
      "defined for this shell"))
  }

  /** Iteration test with iterative Pi example */
  @Test
  def testIterativePIBatch(): Unit = {

    val input: String =
      """
        val initial = benv.fromElements(0)
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
  def testWordCountBatch(): Unit = {
    val input =
      """
        val text = benv.fromElements("To be, or not to be,--that is the question:--",
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
  def testSumBatch: Unit = {
    val input =
      """
        val input: DataSet[Int] = benv.fromElements(0,1,2,3,4,5,6,7,8,9,10)
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
  def testWordCountWithCustomCaseClassBatch: Unit = {
    val input =
      """
      case class WC(word: String, count: Int)
      val wordCounts = benv.fromElements(
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

  @Test
  def testSimpleSelectWithFilterBatchTableAPIQuery: Unit = {
    val input =
      """
        |val data = Seq(
        |    (1, 1L, "Hi"),
        |    (2, 2L, "Hello"),
        |    (3, 2L, "Hello world"))
        |val t = benv.fromCollection(data).toTable(btenv, 'a, 'b, 'c).select('a,'c).where(
        |'a% 2 === 1 )
        |val results = t.toDataSet[Row].collect()
        |results.foreach(println)
        |:q
      """.stripMargin
    val output = processInShell(input)
    Assert.assertFalse(output.toLowerCase.contains("failed"))
    Assert.assertFalse(output.toLowerCase.contains("error"))
    Assert.assertFalse(output.toLowerCase.contains("exception"))
    Assert.assertTrue(output.contains("1,Hi"))
    Assert.assertTrue(output.contains("3,Hello world"))
  }

  @Test
  def testGroupedAggregationStreamTableAPIQuery: Unit = {
    val input =
      """
        |  val data = List(
        |    ("Hello", 1),
        |    ("word", 1),
        |    ("Hello", 1),
        |    ("bark", 1),
        |    ("bark", 1),
        |    ("bark", 1),
        |    ("bark", 1),
        |    ("bark", 1),
        |    ("bark", 1),
        |    ("flink", 1)
        |  )
        | val stream = senv.fromCollection(data)
        | val table = stream.toTable(stenv, 'word, 'num)
        | val resultTable = table.groupBy('word).select('num.sum as 'count).groupBy('count).select(
        | 'count,'count.count as 'frequency)
        | val results = resultTable.toRetractStream[Row]
        | results.print
        | senv.execute
      """.stripMargin
    val output = processInShell(input)
    Assert.assertTrue(output.contains("6,1"))
    Assert.assertTrue(output.contains("1,2"))
    Assert.assertTrue(output.contains("2,1"))
    Assert.assertFalse(output.toLowerCase.contains("failed"))
    Assert.assertFalse(output.toLowerCase.contains("error"))
    Assert.assertFalse(output.toLowerCase.contains("exception"))
  }

  /**
   * Submit external library.
   * Disabled due to FLINK-7111.
   */
  @Ignore
  @Test
  def testSubmissionOfExternalLibraryBatch: Unit = {
    val input =
      """
         import org.apache.flink.api.scala.jar.TestingData
         val source = benv.fromCollection(TestingData.elements)
         source.print()
      """.stripMargin

    val output: String = processInShell(input, Option("customjar-test-jar.jar"))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))


    Assert.assertTrue(output.contains("\nHELLO 42"))
  }

  /**
   * Submit external library.
   * Disabled due to FLINK-7111.
   */
  @Ignore
  @Test
  def testSubmissionOfExternalLibraryStream: Unit = {
    val input =
      """
        import org.apache.flink.api.scala.jar.TestingData
        val source = senv.fromCollection(TestingData.elements)
        source.print()
        senv.execute
      """.stripMargin

    val output: String = processInShell(input, Option("customjar-test-jar.jar"))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("error"))
    Assert.assertFalse(output.contains("Exception"))

    Assert.assertTrue(output.contains("\nHELLO 42"))
  }


  /**
   * tests flink shell startup with remote cluster (starts cluster internally)
   * for both streaming and batch api
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

    val in: BufferedReader = new BufferedReader(
      new StringReader(
        input + "\n"))
    val out: StringWriter = new StringWriter

    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    val oldOut: PrintStream = System.out
    System.setOut(new PrintStream(baos))

    val dir = temporaryFolder.newFolder()
    BootstrapTools.writeConfiguration(configuration, new File(dir, "flink-conf.yaml"))

    val port: Int = clusterResource.getRestAddres.getPort
    val hostname : String = clusterResource.getRestAddres.getHost

    val args = Array(
      "remote",
      hostname,
      Integer.toString(port),
      "--configDir",
      dir.getAbsolutePath)

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

    Assert.assertTrue(output.contains("foobarStreaming"))
    Assert.assertTrue(output.contains("barfooStreaming"))

    Assert.assertFalse(output.contains("failed"))
    Assert.assertFalse(output.contains("Error"))
    Assert.assertFalse(output.contains("ERROR"))
    Assert.assertFalse(output.contains("Exception"))
  }

  @Test
  def testImportJavaCollection(): Unit = {
    val input = """
      import java.util.List
      val jul: List[Int] = new java.util.ArrayList[Int]()
      jul.add(2)
      jul.add(4)
      jul.add(6)
      jul.add(8)
      jul.add(10)
      val str = "the java list size is: " + jul.size
    """.stripMargin

    val output = processInShell(input)

    Assert.assertTrue(output.contains("the java list size is: 5"))
    Assert.assertFalse(output.toLowerCase.contains("failed"))
    Assert.assertFalse(output.toLowerCase.contains("error"))
    Assert.assertFalse(output.toLowerCase.contains("exception"))

  }

  @Test
  def testImplicitConversionBetweenJavaAndScala(): Unit = {
    val input =
      """
        import collection.JavaConversions._
        import scala.collection.mutable.ArrayBuffer
        val jul:java.util.List[Int] = ArrayBuffer(1,2,3,4,5)
        val buf: Seq[Int] = jul
        var sum = 0
        buf.foreach(num => sum += num)
        val str = "sum is: " + sum
        val scala2jul = List(1,2,3)
        scala2jul.add(7)
      """.stripMargin

    val output = processInShell(input)

    Assert.assertTrue(output.contains("sum is: 15"))
    Assert.assertFalse(output.toLowerCase.contains("failed"))
    Assert.assertFalse(output.toLowerCase.contains("error"))
    Assert.assertTrue(output.contains("java.lang.UnsupportedOperationException"))
  }

  @Test
  def testImportPackageConflict(): Unit = {
    val input =
      """
        import org.apache.flink.table.api._
        import java.util.List
        val jul: List[Int] = new java.util.ArrayList[Int]()
        jul.add(2)
        jul.add(4)
        jul.add(6)
        jul.add(8)
        jul.add(10)
        val str = "the java list size is: " + jul.size
      """.stripMargin

    val output = processInShell(input)
    Assert.assertTrue(output.contains("error: object util is not a member of package org.apache." +
      "flink.table.api.java"))
  }

  @Test
  def testGetMultiExecutionEnvironment(): Unit = {
    val input =
      """
        |val newEnv = ExecutionEnvironment.getExecutionEnvironment
      """.stripMargin
    val output = processInShell(input)
    Assert.assertTrue(output.contains("java.lang.UnsupportedOperationException: Execution " +
      "Environment is already defined for this shell."))
  }

}

object ScalaShellITCase {

  val configuration = new Configuration()
  var cluster: Option[MiniCluster] = None

  val parallelism: Int = 4

  val _clusterResource = new MiniClusterResource(new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(parallelism)
      .build())

  @ClassRule
  def clusterResource = _clusterResource

  /**
   * Run the input using a Scala Shell and return the output of the shell.
    *
    * @param input commands to be processed in the shell
   * @return output of shell
   */
  def processInShell(input: String, externalJars: Option[String] = None): String = {
    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val baos = new ByteArrayOutputStream()

    val oldOut = System.out
    System.setOut(new PrintStream(baos))

    val port: Int = clusterResource.getRestAddres.getPort
    val hostname : String = clusterResource.getRestAddres.getHost

    configuration.setString(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    configuration.setBoolean(DeploymentOptions.ATTACHED, true)

    configuration.setString(JobManagerOptions.ADDRESS, hostname)
    configuration.setInteger(JobManagerOptions.PORT, port)

    configuration.setString(RestOptions.ADDRESS, hostname)
    configuration.setInteger(RestOptions.PORT, port)

      val repl = externalJars match {
        case Some(ej) => new FlinkILoop(
          configuration,
          Option(Array(ej)),
          in, new PrintWriter(out))

        case None => new FlinkILoop(
          configuration,
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
