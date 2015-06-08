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

import org.apache.flink.runtime.StreamingMode
import org.apache.flink.test.util.{TestEnvironment, TestBaseUtils, ForkableFlinkMiniCluster, FlinkTestBase}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.duration.FiniteDuration
import scala.tools.nsc.Settings

class ScalaShellITSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  test("Iteration test with iterative Pi example") {

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

    output should not include "failed"
    output should not include "error"
    output should not include "Exception"
  }

  test("WordCount in Shell") {
    val input = """
        val text = env.fromElements("To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,")

        val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
        val result = counts.print()
    """.stripMargin

    val output = processInShell(input)

    output should not include "failed"
    output should not include "error"
    output should not include "Exception"

//    some of the words that should be included
    output should include("(a,1)")
    output should include("(whether,1)")
    output should include("(to,4)")
    output should include("(arrows,1)")
  }

  test("Sum 1..10, should be 55") {
    val input : String =
      """
        val input: DataSet[Int] = env.fromElements(0,1,2,3,4,5,6,7,8,9,10)
        val reduced = input.reduce(_+_)
        reduced.print
      """.stripMargin

    val output : String = processInShell(input)

    output should not include "failed"
    output should not include "error"
    output should not include "Exception"

    output should include("55")
  }

  test("WordCount in Shell with custom case class") {
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

    output should not include "failed"
    output should not include "error"
    output should not include "Exception"

    output should include("WC(hello,1)")
    output should include("WC(world,10)")
  }
  
  
  test("Submit external library") {
    
    val input : String =
      """
        import org.apache.flink.ml.math._
        val denseVectors = env.fromElements(DenseVector(1.0, 2.0, 3.0))
        denseVectors.print()
      """.stripMargin

    // find jar file that contains the ml code
    var externalJar : String = ""
    var folder : File = new File("../flink-ml/target/");
    var listOfFiles : Array[File] = folder.listFiles();
    for(i <- 0 to listOfFiles.length - 1){
      var filename : String = listOfFiles(i).getName();
      if(!filename.contains("test") && !filename.contains("original") && filename.contains(".jar")){
        println("ive found file:" + listOfFiles(i).getAbsolutePath)
        externalJar = listOfFiles(i).getAbsolutePath
      }
    }

    assert(externalJar != "")

    val output : String = processInShell(input,Option(externalJar))

    output should not include "failed"
    output should not include "error"
    output should not include "Exception"

    output should include( "\nDenseVector(1.0, 2.0, 3.0)")
  }

  /**
   * Run the input using a Scala Shell and return the output of the shell.
   * @param input commands to be processed in the shell
   * @return output of shell
   */
  def processInShell(input : String, externalJars : Option[String] = None): String ={
    
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
    
    var repl : FlinkILoop= null 
    
    externalJars match {
      case Some(ej) => repl = new FlinkILoop(
        host, port,  
        Option(Array(ej)), 
        in, new PrintWriter(out))
        
      case None => repl = new FlinkILoop(
        host,port,
        in,new PrintWriter(out))
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

  var cluster: Option[ForkableFlinkMiniCluster] = None
  val parallelism = 4

  override def beforeAll(): Unit = {
    val cl = TestBaseUtils.startCluster(
      1,
      parallelism,
      StreamingMode.BATCH_ONLY,
      false,
      false,
      false)

    val clusterEnvironment = new TestEnvironment(cl, parallelism)
    clusterEnvironment.setAsContext()

    cluster = Some(cl)
  }

  override def afterAll(): Unit = {
    cluster.map(c => TestBaseUtils.stopCluster(c, new FiniteDuration(1000, TimeUnit.SECONDS)))
  }
}
