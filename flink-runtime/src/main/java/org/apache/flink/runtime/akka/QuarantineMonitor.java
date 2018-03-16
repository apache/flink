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

import org.apache.flink.util.Preconditions;

import akka.actor.UntypedActor;
import akka.remote.AssociationErrorEvent;
import akka.remote.transport.Transport;
import org.slf4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The quarantine monitor subscribes to the event bus of the actor system in which it was started.
 * It listens to {@link AssociationErrorEvent} which contain information if we got quarantined
 * or quarantine another remote actor system. If the actor detects that the actor system has been
 * quarantined or quarantined another system, then the {@link QuarantineHandler} is called.
 *
 * <p>IMPORTANT: The implementation if highly specific for Akka 2.3.7. With different version the
 * quarantine state might be detected differently.
 */
public class QuarantineMonitor extends UntypedActor {

	private static final Pattern pattern = Pattern.compile("^Invalid address:\\s+(.*)$");

	private static final String QUARANTINE_MSG = "The remote system has a UID that has been quarantined. Association aborted.";
	private static final String QUARANTINED_MSG = "The remote system has quarantined this system. No further associations to the remote system are possible until this system is restarted.";

	private final QuarantineHandler handler;
	private final Logger log;

	public QuarantineMonitor(QuarantineHandler handler, Logger log) {
		this.handler = Preconditions.checkNotNull(handler);
		this.log = Preconditions.checkNotNull(log);
	}

	@Override
	public void preStart() {
		getContext().system().eventStream().subscribe(getSelf(), AssociationErrorEvent.class);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof AssociationErrorEvent) {
			AssociationErrorEvent associationErrorEvent = (AssociationErrorEvent) message;

			// IMPORTANT: The check for the quarantining event is highly specific to Akka 2.3.7
			// and can change with a different Akka version.
			// It assumes the following:
			// AssociationErrorEvent(InvalidAssociation(InvalidAssociationException(QUARANTINE(D)_MSG))
			if (associationErrorEvent.getCause() != null) {
				Throwable invalidAssociation = associationErrorEvent.getCause();
				Matcher matcher = pattern.matcher(invalidAssociation.getMessage());

				final String remoteSystem;

				if (matcher.find()) {
					remoteSystem = matcher.group(1);
				} else {
					remoteSystem = "Unknown";
				}

				if (invalidAssociation.getCause() instanceof Transport.InvalidAssociationException) {
					Transport.InvalidAssociationException invalidAssociationException = (Transport.InvalidAssociationException) invalidAssociation.getCause();

					// don't hate the player, hate the game! That's the only way to find out if we
					// got quarantined or quarantined another actor system in Akka 2.3.7
					if (QUARANTINE_MSG.equals(invalidAssociationException.getMessage())) {
						handler.hasQuarantined(remoteSystem, getContext().system());
					} else if (QUARANTINED_MSG.equals(invalidAssociationException.getMessage())) {
						handler.wasQuarantinedBy(remoteSystem, getContext().system());
					} else {
						log.debug("The invalid association exception's message could not be matched.", associationErrorEvent);
					}
				} else {
					log.debug("The association error event's root cause is not of type {}.", Transport.InvalidAssociationException.class.getSimpleName(), associationErrorEvent);
				}
			} else {
				log.debug("Received association error event which did not contain a cause.", associationErrorEvent);
			}
		}
	}
}
