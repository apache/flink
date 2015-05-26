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

import java.io._
import java.net.URLClassLoader

import org.apache.flink.api.scala.FlinkILoop
import org.apache.flink.test.util.AbstractMultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{AbstractMultipleProgramsTestBase, MultipleProgramsTestBase}
import org.junit.{BeforeClass, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.Settings


object ScalaShellITSuite {
  @BeforeClass
  def setup() = {
    AbstractMultipleProgramsTestBase.singleActorSystem = false
    AbstractMultipleProgramsTestBase.setup()
  }
}


@RunWith(classOf[Parameterized])
class ScalaShellITSuite(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode){



  /**
   * initializes new local cluster and processes commands given in input
   * @param input commands to be processed in the shell
   * @return output of shell
   */
  def processInShell(input : String): String ={

    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val baos = new ByteArrayOutputStream()

    System.setOut(new PrintStream(baos))

    // new local cluster
    val host = "localhost"
    val port = AbstractMultipleProgramsTestBase
      .cluster
      .getJobManagerRPCPort

    val cl = getClass.getClassLoader
    var paths = new ArrayBuffer[String]
    if (cl.isInstanceOf[URLClassLoader]) {
      val urlLoader = cl.asInstanceOf[URLClassLoader]
      for (url <- urlLoader.getURLs) {
        if (url.getProtocol == "file") {
          paths += url.getFile
        }
      }
    }
    val classpath = paths.mkString(File.pathSeparator)

    val repl = new FlinkILoop(host, port, in, new PrintWriter(out)) //new MyILoop();

    repl.settings = new Settings()

    // enable this line to use scala in intellij
    repl.settings.usejavacp.value = true

    repl.addedClasspath = classpath

    repl.process(repl.settings)

    repl.closeInterpreter

    out.toString + baos.toString()
  }


  def assertContains(message: String, output: String) {
    val isInOutput = output.contains(message)
    assert(isInOutput,
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContain(message: String, output: String) {
    val isInOutput = output.contains(message)
    assert(!isInOutput,
      "Interpreter output contained '" + message + "':\n" + output)
  }



  /*
   * iteration test, with iterative Pi example
   */
  @Test
  def testIterativePiExample(): Unit = {
    val input : String =
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

    val output : String = processInShell(input)
    //assertDoesNotContain("failed",output)
    assertDoesNotContain("error",output)
    assertDoesNotContain("Exception",output)

    assertContains("Job execution switched to status FINISHED.",output)
  }



  /**
   * performs the wordcount example
   */
  @Test
  def testWordCountExample(): Unit = {

    val input : String = """
        val text = env.fromElements("To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,")

        val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
        val result = counts.print()
        """.stripMargin

      val output : String = processInShell(input)
      //assertDoesNotContain("failed",output)
      assertDoesNotContain("error",output)
      assertDoesNotContain("Exception",output)

      assertContains("Job execution switched to status FINISHED.",output)
      assertContains("(a,1)",output)
      assertContains("(whether,1)",output)
      assertContains("(to,4)",output)
      assertContains("(arrows,1)",output);
    }

  /**
   * sums numbers from 0..10, should be 55
   */
  @Test
  def testSumNumbers(): Unit = {

    val input : String =
      """
        val input: DataSet[Int] = env.fromElements(0,1,2,3,4,5,6,7,8,9,10)
        val reduced = input.reduce(_+_)
        reduced.print
      """.stripMargin

    val output : String = processInShell(input)

    assertDoesNotContain("error",output)
    assertDoesNotContain("Exception",output)

    assertContains("Job execution switched to status FINISHED.",output)
    assertContains("55",output)
  }


  /**
   * tests case classes in pseudo wordcount example
   */
  def testCaseClass(): Unit = {
    val input : String =
    """
      case class WC(word: String, count: Int)

      val wordCounts = env.fromElements(
        new WC("hello", 1),
        new WC("world", 2),
        new WC("world", 8))

      val reduced = wordCounts.groupBy(0).sum(1)

      reduced.print()
    """.stripMargin

    val output : String = processInShell(input)

    assertDoesNotContain("error",output)
    assertDoesNotContain("Exception",output)

    assertContains("Job execution switched to status FINISHED.",output)
    assertContains("WC(hello,1)",output)
    assertContains("WC(world,10)",output)
  }
}
