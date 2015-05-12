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


import scala.tools.nsc.Settings

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster

/**
 * Created by Nikolaas Steenbergen on 22-4-15.
 */


object FlinkShell {

  def main(args: Array[String]) {

    // scopt, command line arguments
    case class Config(port: Int = -1,
                      host: String = "none")
    val parser = new scopt.OptionParser[Config] ("scopt") {
      head ("scopt", "3.x")
      opt[Int] ('p', "port") action {
        (x, c) =>
          c.copy (port = x)
      } text ("port specifies port of running JobManager")
      opt[(String)] ('h',"host") action {
        case (x, c) =>
          c.copy (host = x)
      }  text ("host specifies host name of running JobManager")
      help("help") text("prints this usage text")

    }


    // parse arguments
    parser.parse (args, Config () ) map {
      config =>
        startShell(config.host,config.port);
    } getOrElse {
      // arguments are bad, usage message will have been displayed
      println("Could not parse program arguments")
    }
  }


  def startShell(userHost : String, userPort : Int): Unit ={
    println("Starting Flink Shell:")

    var cluster: LocalFlinkMiniCluster = null

    // either port or userhost not specified by user, create new minicluster
    val (host,port) = if (userHost == "none" || userPort == -1 )
    {
      println("Creating new local server")
      cluster = new LocalFlinkMiniCluster(new Configuration, false)
      ("localhost",cluster.getJobManagerRPCPort)
    } else {
      println(s"Connecting to remote server (host: $userHost, port: $userPort).")
      (userHost, userPort)
    }

    // custom shell
    val repl = new FlinkILoop(host, port) //new MyILoop();

    repl.settings = new Settings()

    repl.settings.usejavacp.value = true

    // start scala interpreter shell
    repl.process(repl.settings)

    //repl.initFlinkEnv()

    repl.closeInterpreter()

    if (cluster != null) {
      cluster.stop()
    }

    print(" good bye ..")
  }
}
