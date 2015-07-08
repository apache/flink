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

import java.util.UUID

import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage
import org.apache.flink.runtime.messages.RequiresLeaderSessionID

/** Mixin to filter out [[LeaderSessionMessage]] which contain an invalid leader session id.
  * Messages which contain a valid leader session ID are unwrapped and forwarded to the actor.
  *
  */
trait LeaderSessionMessages extends FlinkActor {
  protected def leaderSessionID: Option[UUID]

  abstract override def receive: Receive = {
    case LeaderSessionMessage(id, msg) =>
      // Filter out messages which have not the correct leader session ID
      (leaderSessionID, id) match {
        case (Some(currentID), Some(msgID)) =>
          if(currentID.equals(msgID)) {
            // correct leader session ID
            super.receive(msg)
          } else {
            // discard message because of incorrect leader session ID
            handleDiscardedMessage(msg)
          }

        case _ => handleDiscardedMessage(msg)
      }
    case msg: RequiresLeaderSessionID =>
      throw new Exception(s"Received a message $msg without a leader session ID, even though" +
        " it requires to have one.")
    case msg =>
      // pass the message to the parent's receive method for further processing
      super.receive(msg)
  }

  private def handleDiscardedMessage(msg: Any): Unit = {
    log.debug(s"Discard message $msg because the leader session ID was not correct.")
  }

  /** Wrap [[RequiresLeaderSessionID]] messages in a [[LeaderSessionMessage]]
    *
    * @param message The message to decorate
    * @return The decorated message
    */
  override def decorateMessage(message: Any): Any = {
    message match {
      case msg: RequiresLeaderSessionID =>
        LeaderSessionMessage(leaderSessionID, super.decorateMessage(msg))

      case msg => super.decorateMessage(msg)
    }
  }
}
