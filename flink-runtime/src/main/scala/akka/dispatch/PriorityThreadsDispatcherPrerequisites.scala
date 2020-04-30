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

/**
  * Composition over [[DefaultDispatcherPrerequisites]] that replaces thread factory with one that
  * allow to configure thread priority.
  *
  * @param newThreadPriority priority that will be set to each newly created thread
  *                          should be between Thread.MIN_PRIORITY and Thread.MAX_PRIORITY
  */
class PriorityThreadsDispatcherPrerequisites(
  prerequisites: DispatcherPrerequisites,
  newThreadPriority: Int) extends DispatcherPrerequisites {

  private val defaultDispatcherPrerequisites : DefaultDispatcherPrerequisites =
    new DefaultDispatcherPrerequisites(
      eventStream = prerequisites.eventStream,
      scheduler = prerequisites.scheduler,
      dynamicAccess = prerequisites.dynamicAccess,
      settings = prerequisites.settings,
      mailboxes = prerequisites.mailboxes,
      defaultExecutionContext = prerequisites.defaultExecutionContext,
      threadFactory = new PriorityThreadFactory(prerequisites, newThreadPriority)
  )

  override def threadFactory : java.util.concurrent.ThreadFactory = {
    defaultDispatcherPrerequisites.threadFactory
  }

  override def eventStream : akka.event.EventStream = {
    defaultDispatcherPrerequisites.eventStream
  }

  override def scheduler : akka.actor.Scheduler = {
    defaultDispatcherPrerequisites.scheduler
  }

  override def dynamicAccess : akka.actor.DynamicAccess = {
    defaultDispatcherPrerequisites.dynamicAccess
  }

  override def settings : akka.actor.ActorSystem.Settings = {
    defaultDispatcherPrerequisites.settings
  }

  override def mailboxes : akka.dispatch.Mailboxes = {
    defaultDispatcherPrerequisites.mailboxes
  }

  override def defaultExecutionContext : scala.Option[scala.concurrent.ExecutionContext] = {
    defaultDispatcherPrerequisites.defaultExecutionContext
  }
}

object PriorityThreadsDispatcherPrerequisites {
  def apply(prerequisites: DispatcherPrerequisites, newThreadPriority: Int):
    PriorityThreadsDispatcherPrerequisites =
      new PriorityThreadsDispatcherPrerequisites(prerequisites, newThreadPriority)
}


