/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.akka.utils;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * General interface used by Receiver Actor to un subscribe.
 */
public class UnsubscribeReceiver implements Serializable {
	private static final long serialVersionUID = 1L;
	private ActorRef receiverActor;

	public UnsubscribeReceiver(ActorRef receiverActor) {
		this.receiverActor = receiverActor;
	}

	public void setReceiverActor(ActorRef receiverActor) {
		this.receiverActor = receiverActor;
	}

	public ActorRef getReceiverActor() {
		return receiverActor;
	}


	@Override
	public boolean equals(Object obj) {
		if (obj instanceof UnsubscribeReceiver) {
			UnsubscribeReceiver other = (UnsubscribeReceiver) obj;
			return other.canEquals(this) && super.equals(other)
				&& receiverActor.equals(other.getReceiverActor());
		} else {
			return false;
		}
	}

	public boolean canEquals(Object obj) {
		return obj instanceof UnsubscribeReceiver;
	}
}
