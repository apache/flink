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

package org.apache.flink.runtime.akka;

import akka.actor.UntypedActor;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.UUID;

/**
 * Base class for Flink's actors implemented with Java. Actors inheriting from this class
 * automatically log received messages when the debug log level is activated. Furthermore,
 * they filter out {@link LeaderSessionMessage} with the wrong leader session ID. If a message
 * of type {@link RequiresLeaderSessionID} without being wrapped in a LeaderSessionMessage is
 * detected, then an Exception is thrown.
 *
 * In order to implement the actor behavior, an implementing subclass has to override the method
 * handleMessage, which defines how messages are processed. Furthermore, the subclass has to provide
 * a leader session ID option which is returned by getLeaderSessionID.
 */
public abstract class FlinkUntypedActor extends UntypedActor {
	
	protected final Logger LOG = LoggerFactory.getLogger(getClass());

	/**
	 * This method is called by Akka if a new message has arrived for the actor. It logs the
	 * processing time of the incoming message if the logging level is set to debug. After logging
	 * the handleLeaderSessionID method is called.
	 *
	 * Important: This method cannot be overriden. The actor specific message handling logic is
	 * implemented by the method handleMessage.
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	@Override
	public final void onReceive(Object message) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received message {} at {} from {}.", message, getSelf().path(), getSender());

			long start = System.nanoTime();

			handleLeaderSessionID(message);

			long duration = (System.nanoTime() - start)/ 1000000;

			LOG.debug("Handled message {} in {} ms from {}.", message, duration, getSender());
		} else {
			handleLeaderSessionID(message);
		}
	}

	/**
	 * This method filters out {@link LeaderSessionMessage} whose leader session ID is not equal
	 * to the actors leader session ID. If a message of type {@link RequiresLeaderSessionID}
	 * arrives, then an Exception is thrown, because these messages have to be wrapped in a
	 * {@link LeaderSessionMessage}.
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	private void handleLeaderSessionID(Object message) throws Exception {
		if(message instanceof LeaderSessionMessage) {
			LeaderSessionMessage msg = (LeaderSessionMessage) message;

			if(msg.leaderSessionID().isDefined() && getLeaderSessionID().isDefined()) {
				if(getLeaderSessionID().equals(msg.leaderSessionID())) {
					// finally call method to handle message
					handleMessage(msg.message());
				} else {
					handleDiscardedMessage(msg);
				}
			} else {
				handleDiscardedMessage(msg);
			}
		} else if (message instanceof RequiresLeaderSessionID) {
			throw new Exception("Received a message " + message + " without a leader session " +
					"ID, even though it requires to have one.");
		} else {
			// call method to handle message
			handleMessage(message);
		}
	}

	private void handleDiscardedMessage(Object msg) {
		LOG.debug("Discard message {} because the leader session ID was not correct.", msg);
	}

	/**
	 * This method contains the actor logic which defines how to react to incoming messages.
	 *
	 * @param message Incoming message
	 * @throws Exception
	 */
	protected abstract void handleMessage(Object message) throws Exception;

	/**
	 * Returns the current leader session ID associcated with this actor.
	 * @return
	 */
	protected abstract Option<UUID> getLeaderSessionID();

	/**
	 * This method should be called for every outgoing message. It wraps messages which require
	 * a leader session ID (indicated by {@link RequiresLeaderSessionID}) in a
	 * {@link LeaderSessionMessage} with the actor's leader session ID.
	 *
	 * This method can be overriden to implement a different decoration behavior.
	 *
	 * @param message Message to be decorated
	 * @return The deocrated message
	 */
	protected Object decorateMessage(Object message) {
		if(message instanceof  RequiresLeaderSessionID) {
			return new LeaderSessionMessage(getLeaderSessionID(), message);
		} else {
			return message;
		}
	}

}
