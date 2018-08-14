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

package akka.actor

import akka.actor.RobustActorSystem.LOG
import akka.actor.setup.ActorSystemSetup
import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logger

/**
  * A robust actor system which can caught the escape exception
  *
  * @param name the actor system name.
  * @param applicationConfig the actor system configuration.
  */
class RobustActorSystem(name: String, applicationConfig: Config)
  extends ActorSystemImpl(name, applicationConfig, ActorSystem.findClassLoader(),
    None, guardianProps = None, ActorSystemSetup.empty) {

  override protected def uncaughtExceptionHandler: Thread.UncaughtExceptionHandler =
    new Thread.UncaughtExceptionHandler() {

      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        try
          LOG.error("Thread " +  t.getName + " died due to an uncaught exception. Killing process.")
        finally Runtime.getRuntime.halt(-1)
      }

    }
}

object RobustActorSystem {

  private val LOG = Logger(classOf[RobustActorSystem])

  def apply(name: String = "default"): ActorSystem = {
    val classLoader = ActorSystem.findClassLoader()
    apply(name, ConfigFactory.load(classLoader), classLoader)
  }

  def apply(name: String, config: Config, classLoader: ClassLoader): ActorSystem = new RobustActorSystem(name, config).start()

}

