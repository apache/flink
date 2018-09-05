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

import java.lang.Thread.UncaughtExceptionHandler

import akka.actor.ActorSystem.findClassLoader
import akka.actor.setup.ActorSystemSetup
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.runtime.util.FatalExitExceptionHandler

import scala.concurrent.ExecutionContext

/**
  * [[ActorSystemImpl]] which has a configurable [[java.lang.Thread.UncaughtExceptionHandler]].
  */
class RobustActorSystem(
    name: String,
    applicationConfig: Config,
    classLoader: ClassLoader,
    defaultExecutionContext: Option[ExecutionContext],
    guardianProps: Option[Props],
    setup: ActorSystemSetup,
    val optionalUncaughtExceptionHandler: Option[UncaughtExceptionHandler])
    extends ActorSystemImpl(
      name,
      applicationConfig,
      classLoader,
      defaultExecutionContext,
      guardianProps,
      setup) {

  override protected def uncaughtExceptionHandler: Thread.UncaughtExceptionHandler =
    optionalUncaughtExceptionHandler.getOrElse(super.uncaughtExceptionHandler)
}

object RobustActorSystem {
  def create(name: String, applicationConfig: Config): RobustActorSystem = {
    apply(name, ActorSystemSetup.create(BootstrapSetup(None, Option(applicationConfig), None)))
  }

  def create(
      name: String,
      applicationConfig: Config,
      uncaughtExceptionHandler: UncaughtExceptionHandler): RobustActorSystem = {
    apply(
      name,
      ActorSystemSetup.create(BootstrapSetup(None, Option(applicationConfig), None)),
      uncaughtExceptionHandler
    )
  }

  def apply(name: String, setup: ActorSystemSetup): RobustActorSystem = {
    internalApply(name, setup, Some(FatalExitExceptionHandler.INSTANCE))
  }

  def apply(
      name: String,
      setup: ActorSystemSetup,
      uncaughtExceptionHandler: UncaughtExceptionHandler): RobustActorSystem = {
    internalApply(name, setup, Some(uncaughtExceptionHandler))
  }

  def internalApply(
      name: String,
      setup: ActorSystemSetup,
      uncaughtExceptionHandler: Option[UncaughtExceptionHandler]): RobustActorSystem = {
    val bootstrapSettings = setup.get[BootstrapSetup]
    val cl = bootstrapSettings.flatMap(_.classLoader).getOrElse(findClassLoader())
    val appConfig = bootstrapSettings.flatMap(_.config).getOrElse(ConfigFactory.load(cl))
    val defaultEC = bootstrapSettings.flatMap(_.defaultExecutionContext)

    new RobustActorSystem(
      name,
      appConfig,
      cl,
      defaultEC,
      None,
      setup,
      uncaughtExceptionHandler).start()
  }
}
