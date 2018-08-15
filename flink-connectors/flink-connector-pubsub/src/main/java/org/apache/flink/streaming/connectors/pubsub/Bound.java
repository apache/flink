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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

class Bound<OUT> implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(Bound.class);

	private final Bound.Mode mode;
	private final long maxMessagedReceived;
	private final long maxTimeBetweenMessages;

	private SourceFunction<OUT> sourceFunction;
	private transient Timer timer;
	private long messagesReceived;
	private long lastReceivedMessage;
	private boolean cancelled = false;

	private Bound(Bound.Mode mode, long maxMessagedReceived, long maxTimeBetweenMessages) {
		this.mode = mode;
		this.maxMessagedReceived = maxMessagedReceived;
		this.maxTimeBetweenMessages = maxTimeBetweenMessages;
		this.messagesReceived = 0L;
	}

	static <OUT> Bound<OUT> boundByAmountOfMessages(long maxMessagedReceived) {
		return new Bound<>(Mode.COUNTER, maxMessagedReceived, 0L);
	}

	static <OUT> Bound<OUT> boundByTimeSinceLastMessage(long maxTimeBetweenMessages) {
		return new Bound<>(Mode.TIMER, 0L, maxTimeBetweenMessages);
	}

	static <OUT> Bound<OUT> boundByAmountOfMessagesOrTimeSinceLastMessage(long maxMessagedReceived, long maxTimeBetweenMessages) {
		return new Bound<>(Mode.COUNTER_OR_TIMER, maxMessagedReceived, maxTimeBetweenMessages);
	}

	private TimerTask shutdownPubSubSource() {
		return new TimerTask() {
			@Override
			public void run() {
				if (maxTimeBetweenMessagesElapsed()) {
					cancelPubSubSource("BoundedSourceFunction: Idle timeout --> canceling source");
					timer.cancel();
				}
			}
		};
	}

	private synchronized boolean maxTimeBetweenMessagesElapsed() {
		return System.currentTimeMillis() - lastReceivedMessage > maxTimeBetweenMessages;
	}

	private synchronized void cancelPubSubSource(String logMessage) {
		if (!cancelled) {
			cancelled = true;
			sourceFunction.cancel();
			LOG.info(logMessage);
		}
	}

	void start(SourceFunction<OUT> sourceFunction) {
		if (this.sourceFunction != null) {
			throw new IllegalStateException("start() already called");
		}

		this.sourceFunction = sourceFunction;
		messagesReceived = 0;

		if (mode == Mode.TIMER || mode == Mode.COUNTER_OR_TIMER) {
			lastReceivedMessage = System.currentTimeMillis();
			timer = new Timer();
			timer.schedule(shutdownPubSubSource(), 0, 100);
		}
	}

	synchronized void receivedMessage() {
		if (sourceFunction == null) {
			throw new IllegalStateException("start() not called");
		}

		lastReceivedMessage = System.currentTimeMillis();
		messagesReceived++;

		if ((mode == Mode.COUNTER || mode == Mode.COUNTER_OR_TIMER) && messagesReceived >= maxMessagedReceived) {
			cancelPubSubSource("BoundedSourceFunction: Max received messages --> canceling source");
		}
	}

	private enum Mode {
		COUNTER, TIMER, COUNTER_OR_TIMER
	}
}
