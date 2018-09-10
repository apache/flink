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

package org.apache.flink.runtime.jobmanager

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.flink.configuration.{AkkaOptions, ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.util.NetUtils
import org.junit.Assert._
import org.junit.Test

import scala.concurrent.duration.Duration

/**
 * Tests that a lookup of a local JobManager fails within a given timeout if the JobManager
 * actor is not reachable.
 */
class JobManagerConnectionTest {

  private val timeout = 1000

  @Test
  def testResolveUnreachableActorLocalHost() : Unit = {
    // startup a test actor system listening at an arbitrary address
    val actorSystem = AkkaUtils.createActorSystem(new Configuration(), Some(("", 0)))

    try {
      // get a port that we know is unoccupied
      val freePort = try {
        NetUtils.getAvailablePort()
      } catch {
        // abort the test if we cannot find a free port
        case _ : Throwable => return
      }

      val endpoint = NetUtils.unresolvedHostAndPortToNormalizedString("127.0.0.1", freePort)
      val config = createConfigWithLowTimeout()

      mustReturnWithinTimeout(Duration(5*timeout, TimeUnit.MILLISECONDS)) {
        () => {
          try {
            AkkaUtils.getActorRef(
              endpoint,
              actorSystem,
              AkkaUtils.getLookupTimeout(config))
            fail("Should fail since the JobManager is not reachable")
          }
          catch {
            case e: IOException => // good
          }
        }
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        fail(e.getMessage)
    }
    finally {
      actorSystem.terminate()
    }
  }

  /**
   * Tests that a lookup of a local JobManager fails within a given timeout if the JobManager
   * actor is not reachable.
   */
  @Test
  def testResolveUnreachableActorRemoteHost() : Unit = {
    // startup a test actor system listening at an arbitrary address
    val actorSystem = AkkaUtils.createActorSystem(new Configuration(), Some(("", 0)))

    try {
      // some address that is not running a JobManager
      val endpoint = NetUtils.unresolvedHostAndPortToNormalizedString("10.254.254.254", 2)
      val config = createConfigWithLowTimeout()

      mustReturnWithinTimeout(Duration(5*timeout, TimeUnit.MILLISECONDS)) {
        () => {
          try {
            AkkaUtils.getActorRef(
              endpoint,
              actorSystem,
              AkkaUtils.getLookupTimeout(config))
            fail("Should fail since the JobManager is not reachable")
          }
          catch {
            case e: IOException => // good
          }
        }
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        fail(e.getMessage)
    }
    finally {
      actorSystem.terminate()
    }
  }

  private def createConfigWithLowTimeout() : Configuration = {
    val config = new Configuration()
    config.setString(AkkaOptions.LOOKUP_TIMEOUT,
                     Duration(timeout, TimeUnit.MILLISECONDS).toSeconds + " s")
    config
  }

  private def mustReturnWithinTimeout(timeout: Duration)(task: () => Unit) : Unit = {

    val done = new AtomicBoolean()
    val error = new AtomicReference[Throwable]()

    val runnable = new Runnable {
      override def run(): Unit = {
        try {
          task()
          done.set(true)
        }
        catch {
          case t: Throwable => error.set(t)
        }
        done.synchronized {
          done.notifyAll()
        }
      }
    }

    val runner = new Thread(runnable, "Test runner")
    runner.setDaemon(true)

    var now = System.currentTimeMillis()
    val deadline = now + timeout.toMillis

    runner.start()

    done.synchronized {
      while (error.get() == null && !done.get() && now < deadline) {
        done.wait(deadline - now)
        now = System.currentTimeMillis()
      }
    }

    if (error.get() != null) {
      error.get().printStackTrace()
      fail("Exception in the timed call: " + error.get().getMessage())
    }

    // check if we finished because we were done
    // otherwise it is a timeout
    if (!done.get()) {
      runner.interrupt()
      fail("Call did not finish within " + timeout)
    }
  }
}
