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

import java.io.{BufferedReader, File, FileOutputStream}

import scala.tools.nsc.interpreter._

import org.apache.flink.api.java.{JarHelper, ScalaShellRemoteEnvironment}
import org.apache.flink.util.AbstractID


class FlinkILoop(
    val host: String,
    val port: Int,
    val externalJars: Option[Array[String]],
    in0: Option[BufferedReader],
    out0: JPrintWriter)
  extends ILoop(in0, out0) {
  
  
  
  def this(host:String, 
           port:Int, 
           externalJars : Option[Array[String]], 
           in0: BufferedReader, 
           out: JPrintWriter){
    this(host:String, port:Int, externalJars, Some(in0), out)
  }

  def this(host:String, port:Int, externalJars : Option[Array[String]]){
    this(host:String,port: Int, externalJars , None, new JPrintWriter(Console.out, true))
  }
  
  def this(host: String, port: Int, in0: BufferedReader, out: JPrintWriter){
    this(host: String, port: Int, None, in0: BufferedReader, out: JPrintWriter)
  }
  // remote environment
  private val remoteEnv: ScalaShellRemoteEnvironment = {
    val remoteEnv = new ScalaShellRemoteEnvironment(host, port, this)
    remoteEnv
  }

  // local environment
  val scalaEnv: ExecutionEnvironment = {
    val scalaEnv = new ExecutionEnvironment(remoteEnv)
    scalaEnv
  }

  addThunk {
    intp.beQuietDuring {
      // automatically imports the flink scala api
      intp.addImports("org.apache.flink.api.scala._")
      intp.addImports("org.apache.flink.api.common.functions._")
      // with this we can access this object in the scala shell
      intp.bindValue("env", this.scalaEnv)
    }
  }


  /**
   * creates a temporary directory to store compiled console files
   */
  private val tmpDirBase: File = {
    // get unique temporary folder:
    val abstractID: String = new AbstractID().toString
    val tmpDir: File = new File(
      System.getProperty("java.io.tmpdir"),
      "scala_shell_tmp-" + abstractID)
    if (!tmpDir.exists) {
      tmpDir.mkdir
    }
    tmpDir
  }

  // scala_shell commands
  private val tmpDirShell: File = {
    new File(tmpDirBase, "scala_shell_commands")
  }

  // scala shell jar file name
  private val tmpJarShell: File = {
    new File(tmpDirBase, "scala_shell_commands.jar")
  }


  /**
   * Packages the compiled classes of the current shell session into a Jar file for execution
   * on a Flink cluster.
   *
   * @return The path of the created Jar file
   */
  def writeFilesToDisk(): File = {
    val vd = intp.virtualDirectory

    val vdIt = vd.iterator

    for (fi <- vdIt) {
      if (fi.isDirectory) {

        val fiIt = fi.iterator

        for (f <- fiIt) {

          // directory for compiled line
          val lineDir = new File(tmpDirShell.getAbsolutePath, fi.name)
          lineDir.mkdirs()

          // compiled classes for commands from shell
          val writeFile = new File(lineDir.getAbsolutePath, f.name)
          val outputStream = new FileOutputStream(writeFile)
          val inputStream = f.input

          // copy file contents
          org.apache.commons.io.IOUtils.copy(inputStream, outputStream)

          inputStream.close()
          outputStream.close()
        }
      }
    }

    val compiledClasses = new File(tmpDirShell.getAbsolutePath)

    val jarFilePath = new File(tmpJarShell.getAbsolutePath)

    val jh: JarHelper = new JarHelper
    jh.jarDir(compiledClasses, jarFilePath)

    jarFilePath
  }

  /**
   * CUSTOM START METHODS OVERRIDE:
   */
  override def prompt = "Scala-Flink> "

  /**
   * custom welcome message
   */
  override def printWelcome() {
    echo(
      // scalastyle:off
      """
                         \u2592\u2593\u2588\u2588\u2593\u2588\u2588\u2592
                     \u2593\u2588\u2588\u2588\u2588\u2592\u2592\u2588\u2593\u2592\u2593\u2588\u2588\u2588\u2593\u2592
                  \u2593\u2588\u2588\u2588\u2593\u2591\u2591        \u2592\u2592\u2592\u2593\u2588\u2588\u2592  \u2592
                \u2591\u2588\u2588\u2592   \u2592\u2592\u2593\u2593\u2588\u2593\u2593\u2592\u2591      \u2592\u2588\u2588\u2588\u2588
                \u2588\u2588\u2592         \u2591\u2592\u2593\u2588\u2588\u2588\u2592    \u2592\u2588\u2592\u2588\u2592
                  \u2591\u2593\u2588            \u2588\u2588\u2588   \u2593\u2591\u2592\u2588\u2588
                    \u2593\u2588       \u2592\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591\u2592\u2591\u2593\u2593\u2588
                  \u2588\u2591 \u2588   \u2592\u2592\u2591       \u2588\u2588\u2588\u2593\u2593\u2588 \u2592\u2588\u2592\u2592\u2592
                  \u2588\u2588\u2588\u2588\u2591   \u2592\u2593\u2588\u2593      \u2588\u2588\u2592\u2592\u2592 \u2593\u2588\u2588\u2588\u2592
               \u2591\u2592\u2588\u2593\u2593\u2588\u2588       \u2593\u2588\u2592    \u2593\u2588\u2592\u2593\u2588\u2588\u2593 \u2591\u2588\u2591
         \u2593\u2591\u2592\u2593\u2588\u2588\u2588\u2588\u2592 \u2588\u2588         \u2592\u2588    \u2588\u2593\u2591\u2592\u2588\u2592\u2591\u2592\u2588\u2592
        \u2588\u2588\u2588\u2593\u2591\u2588\u2588\u2593  \u2593\u2588           \u2588   \u2588\u2593 \u2592\u2593\u2588\u2593\u2593\u2588\u2592
      \u2591\u2588\u2588\u2593  \u2591\u2588\u2591            \u2588  \u2588\u2592 \u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2592 \u2588\u2588\u2593\u2591\u2592
     \u2588\u2588\u2588\u2591 \u2591 \u2588\u2591          \u2593 \u2591\u2588 \u2588\u2588\u2588\u2588\u2588\u2592\u2591\u2591    \u2591\u2588\u2591\u2593  \u2593\u2591
    \u2588\u2588\u2593\u2588 \u2592\u2592\u2593\u2592          \u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2591       \u2592\u2588\u2592 \u2592\u2593 \u2593\u2588\u2588\u2593
 \u2592\u2588\u2588\u2593 \u2593\u2588 \u2588\u2593\u2588       \u2591\u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2593\u2592\u2591         \u2588\u2588\u2592\u2592  \u2588 \u2592  \u2593\u2588\u2592
 \u2593\u2588\u2593  \u2593\u2588 \u2588\u2588\u2593 \u2591\u2593\u2593\u2593\u2593\u2593\u2593\u2593\u2592              \u2592\u2588\u2588\u2593           \u2591\u2588\u2592
 \u2593\u2588    \u2588 \u2593\u2588\u2588\u2588\u2593\u2592\u2591              \u2591\u2593\u2593\u2593\u2588\u2588\u2588\u2593          \u2591\u2592\u2591 \u2593\u2588
 \u2588\u2588\u2593    \u2588\u2588\u2592    \u2591\u2592\u2593\u2593\u2588\u2588\u2588\u2593\u2593\u2593\u2593\u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2592            \u2593\u2588\u2588\u2588  \u2588
\u2593\u2588\u2588\u2588\u2592 \u2588\u2588\u2588   \u2591\u2593\u2593\u2592\u2591\u2591   \u2591\u2593\u2588\u2588\u2588\u2588\u2593\u2591                  \u2591\u2592\u2593\u2592  \u2588\u2593
\u2588\u2593\u2592\u2592\u2593\u2593\u2588\u2588  \u2591\u2592\u2592\u2591\u2591\u2591\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591                            \u2588\u2593
\u2588\u2588 \u2593\u2591\u2592\u2588   \u2593\u2593\u2593\u2593\u2592\u2591\u2591  \u2592\u2588\u2593       \u2592\u2593\u2593\u2588\u2588\u2593    \u2593\u2592          \u2592\u2592\u2593
\u2593\u2588\u2593 \u2593\u2592\u2588  \u2588\u2593\u2591  \u2591\u2592\u2593\u2593\u2588\u2588\u2592            \u2591\u2593\u2588\u2592   \u2592\u2592\u2592\u2591\u2592\u2592\u2593\u2588\u2588\u2588\u2588\u2588\u2592
 \u2588\u2588\u2591 \u2593\u2588\u2592\u2588\u2592  \u2592\u2593\u2593\u2592  \u2593\u2588                \u2588\u2591      \u2591\u2591\u2591\u2591   \u2591\u2588\u2592
 \u2593\u2588   \u2592\u2588\u2593   \u2591     \u2588\u2591                \u2592\u2588              \u2588\u2593
  \u2588\u2593   \u2588\u2588         \u2588\u2591                 \u2593\u2593        \u2592\u2588\u2593\u2593\u2593\u2592\u2588\u2591
   \u2588\u2593 \u2591\u2593\u2588\u2588\u2591       \u2593\u2592                  \u2593\u2588\u2593\u2592\u2591\u2591\u2591\u2592\u2593\u2588\u2591    \u2592\u2588
    \u2588\u2588   \u2593\u2588\u2593\u2591      \u2592                    \u2591\u2592\u2588\u2592\u2588\u2588\u2592      \u2593\u2593
     \u2593\u2588\u2592   \u2592\u2588\u2593\u2592\u2591                         \u2592\u2592 \u2588\u2592\u2588\u2593\u2592\u2592\u2591\u2591\u2592\u2588\u2588
      \u2591\u2588\u2588\u2592    \u2592\u2593\u2593\u2592                     \u2593\u2588\u2588\u2593\u2592\u2588\u2592 \u2591\u2593\u2593\u2593\u2593\u2592\u2588\u2593
        \u2591\u2593\u2588\u2588\u2592                          \u2593\u2591  \u2592\u2588\u2593\u2588  \u2591\u2591\u2592\u2592\u2592
            \u2592\u2593\u2593\u2593\u2593\u2593\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2591\u2591\u2593\u2593  \u2593\u2591\u2592\u2588\u2591

              F L I N K - S C A L A - S H E L L

NOTE: Use the prebound Execution Environment "env" to read data and execute your program:
  * env.readTextFile("/path/to/data")
  * env.execute("Program name")

HINT: You can use print() on a DataSet to print the contents to this shell.
      """
    // scalastyle:on
    )

  }

  def getExternalJars(): Array[String] = externalJars.getOrElse(Array.empty[String])

}

