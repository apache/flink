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

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter._

import org.apache.flink.api.java.ScalaShellRemoteEnvironment
import org.apache.flink.util.AbstractID


class FlinkILoop(val host: String,
                 val port: Int,
                 in0: Option[BufferedReader],
                 out0: JPrintWriter)
  extends ILoop(in0, out0) {

  def this(host:String, port:Int, in0: BufferedReader, out: JPrintWriter){
    this(host:String, port:Int, Some(in0), out)
  }

  def this(host:String, port:Int){
    this(host:String,port: Int,None, new JPrintWriter(Console.out, true))
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


  /**
   * CUSTOM START METHODS OVERRIDE:
   */

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
   * writes contents of the compiled lines that have been executed in the shell into a
   * "physical directory": creates a unique temporary directory
   */
  def writeFilesToDisk(): Unit = {
    val vd = intp.virtualDirectory

    var vdIt = vd.iterator

    for (fi <- vdIt) {
      if (fi.isDirectory) {

        var fiIt = fi.iterator

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
      """
                         ▒▓██▓██▒
                     ▓████▒▒█▓▒▓███▓▒
                  ▓███▓░░        ▒▒▒▓██▒  ▒
                ░██▒   ▒▒▓▓█▓▓▒░      ▒████
                ██▒         ░▒▓███▒    ▒█▒█▒
                  ░▓█            ███   ▓░▒██
                    ▓█       ▒▒▒▒▒▓██▓░▒░▓▓█
                  █░ █   ▒▒░       ███▓▓█ ▒█▒▒▒
                  ████░   ▒▓█▓      ██▒▒▒ ▓███▒
               ░▒█▓▓██       ▓█▒    ▓█▒▓██▓ ░█░
         ▓░▒▓████▒ ██         ▒█    █▓░▒█▒░▒█▒
        ███▓░██▓  ▓█           █   █▓ ▒▓█▓▓█▒
      ░██▓  ░█░            █  █▒ ▒█████▓▒ ██▓░▒
     ███░ ░ █░          ▓ ░█ █████▒░░    ░█░▓  ▓░
    ██▓█ ▒▒▓▒          ▓███████▓░       ▒█▒ ▒▓ ▓██▓
 ▒██▓ ▓█ █▓█       ░▒█████▓▓▒░         ██▒▒  █ ▒  ▓█▒
 ▓█▓  ▓█ ██▓ ░▓▓▓▓▓▓▓▒              ▒██▓           ░█▒
 ▓█    █ ▓███▓▒░              ░▓▓▓███▓          ░▒░ ▓█
 ██▓    ██▒    ░▒▓▓███▓▓▓▓▓██████▓▒            ▓███  █
▓███▒ ███   ░▓▓▒░░   ░▓████▓░                  ░▒▓▒  █▓
█▓▒▒▓▓██  ░▒▒░░░▒▒▒▒▓██▓░                            █▓
██ ▓░▒█   ▓▓▓▓▒░░  ▒█▓       ▒▓▓██▓    ▓▒          ▒▒▓
▓█▓ ▓▒█  █▓░  ░▒▓▓██▒            ░▓█▒   ▒▒▒░▒▒▓█████▒
 ██░ ▓█▒█▒  ▒▓▓▒  ▓█                █░      ░░░░   ░█▒
 ▓█   ▒█▓   ░     █░                ▒█              █▓
  █▓   ██         █░                 ▓▓        ▒█▓▓▓▒█░
   █▓ ░▓██░       ▓▒                  ▓█▓▒░░░▒▓█░    ▒█
    ██   ▓█▓░      ▒                    ░▒█▒██▒      ▓▓
     ▓█▒   ▒█▓▒░                         ▒▒ █▒█▓▒▒░░▒██
      ░██▒    ▒▓▓▒                     ▓██▓▒█▒ ░▓▓▓▓▒█▓
        ░▓██▒                          ▓░  ▒█▓█  ░░▒▒▒
            ▒▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░▓▓  ▓░▒█░

              F L I N K - S C A L A - S H E L L

NOTE: Use the prebound Execution Environment "env" to read data and execute your program:
  * env.readTextFile("/path/to/data")
  * env.execute("Program name")

HINT: You can use print() on a DataSet to print the contents to this shell.
      """)
  }

  //  getter functions:
  // get (root temporary folder)
  def getTmpDirBase(): File = {
    return (this.tmpDirBase);
  }

  // get shell folder name inside tmp dir
  def getTmpDirShell(): File = {
    return (this.tmpDirShell)
  }

  // get tmp jar file name
  def getTmpJarShell(): File = {
    return (this.tmpJarShell)
  }
}
