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

package akka.dispatch

import com.typesafe.config.Config

/**
  * Akka Dispatcher that creates thread with configurable priority.
  *
  * Example of configuration:
  *
  *   low-priority-threads-dispatcher {
  *     type = akka.dispatch.PriorityThreadsDispatcher
  *     executor = "thread-pool-executor"
  *     # should be between Thread.MIN_PRIORITY (which is 1) and Thread.MAX_PRIORITY (which is 10)
  *     threads-priority = 1
  *     thread-pool-executor {
  *       core-pool-size-min = 0
  *       core-pool-size-factor = 2.0
  *       core-pool-size-max = 10
  *     }
  *   }
  *
  * Two arguments constructor (the primary constructor) is automatically called by Akka
  * when it founds:
  *   abcde-dispatcher {
  *     type = akka.dispatch.PriorityThreadsDispatcher <-- the class that Akka will instantiate
  *     ...
  *   }
  *
  * @param config passed automatically by Akka, should contains information about threads priority
  * @param prerequisites passed automatically by Akka
  */
class PriorityThreadsDispatcher(config: Config, prerequisites: DispatcherPrerequisites)
  extends DispatcherConfigurator(
    config,
    new PriorityThreadsDispatcherPrerequisites(
      prerequisites,
      config.getInt(PriorityThreadsDispatcher.threadPriorityConfigKey)
    )
  )

object PriorityThreadsDispatcher {
  /**
    * Configuration key under which int value should be placed.
    */
  val threadPriorityConfigKey = "thread-priority"
}
