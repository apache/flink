/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.messages.Message;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.DeadLetter;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Actor which listens to {@link org.apache.pekko.actor.DeadLetter} and responds with a failure if
 * the message was a RPC.
 */
public class DeadLettersActor extends AbstractActor {

    private static final Logger LOG = LoggerFactory.getLogger(DeadLettersActor.class);

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder().match(DeadLetter.class, this::handleDeadLetter).build();
    }

    private void handleDeadLetter(DeadLetter deadLetter) {
        if (deadLetter.message() instanceof Message) {
            if (deadLetter.sender().equals(getContext().getSystem().deadLetters())) {
                LOG.debug(
                        "Could not deliver message {} with no sender to recipient {}. "
                                + "This indicates that the actor terminated unexpectedly.",
                        deadLetter.message(),
                        deadLetter.recipient());
            } else {
                deadLetter
                        .sender()
                        .tell(
                                new Status.Failure(
                                        new RecipientUnreachableException(
                                                deadLetter.sender().toString(),
                                                deadLetter.recipient().toString(),
                                                deadLetter.message().toString())),
                                getSelf());
            }
        }
    }

    public static Props getProps() {
        return Props.create(DeadLettersActor.class);
    }
}
