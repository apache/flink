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

package org.apache.flink.runtime

import _root_.akka.actor.Actor
import _root_.akka.event.LoggingAdapter

trait ActorLogMessages {
  self: Actor =>

  override def receive: Receive = new Actor.Receive {
    private val _receiveWithLogMessages = receiveWithLogMessages

    override def isDefinedAt(x: Any): Boolean = _receiveWithLogMessages.isDefinedAt(x)

    override def apply(x: Any):Unit = {
      if (!log.isDebugEnabled) {
        _receiveWithLogMessages(x)
      }
      else {
        log.debug(s"Received message ${x} from ${self.sender}.")
        
        val start = System.nanoTime()
        
        _receiveWithLogMessages(x)
        
        val duration = (System.nanoTime() - start) / 1000000
        log.debug(s"Handled message ${x} in ${duration} ms from ${self.sender}.")
      }
    }
  }

  def receiveWithLogMessages: Receive

  protected def log: LoggingAdapter
}
