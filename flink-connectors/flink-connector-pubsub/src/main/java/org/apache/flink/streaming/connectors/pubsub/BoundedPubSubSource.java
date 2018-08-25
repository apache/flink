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

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;

/**
 * A bounded PubSub Source, similar to {@link PubSubSource} but this will stop at some point.
 * For example after a period of being idle or and after n amount of messages have been received.
 */
public class BoundedPubSubSource<OUT> extends PubSubSource<OUT> {
	private Bound<OUT> bound;

	private BoundedPubSubSource() {
		super();
	}

	protected void setBound(Bound<OUT> bound) {
		this.bound = bound;
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) {
		bound.start(this);
		super.run(sourceContext);
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		super.receiveMessage(message, consumer);
		bound.receivedMessage();
	}

	/**
	 * Creates a {@link BoundedPubSubSourceBuilder}.
	 * @param <OUT> Type of Object which will be read by the produced {@link BoundedPubSubSource}
	 */
	@SuppressWarnings("unchecked")
	public static <OUT> BoundedPubSubSourceBuilder<OUT, ? extends PubSubSource, ? extends BoundedPubSubSourceBuilder> newBuilder() {
		return new BoundedPubSubSourceBuilder<>(new BoundedPubSubSource<OUT>());
	}

	/**
	 * Builder to create BoundedPubSubSource.
	 * @param <OUT> Type of Object which will be read by the BoundedPubSubSource
	 */
	@SuppressWarnings("unchecked")
	public static class BoundedPubSubSourceBuilder<OUT, PSS extends BoundedPubSubSource<OUT>, BUILDER extends BoundedPubSubSourceBuilder<OUT, PSS, BUILDER>>
			extends PubSubSourceBuilder<OUT, PSS, BUILDER> {
		private Long boundedByAmountOfMessages;
		private Long boundedByTimeSinceLastMessage;

		BoundedPubSubSourceBuilder(PSS sourceUnderConstruction) {
			super(sourceUnderConstruction);
		}

		public BUILDER boundedByAmountOfMessages(long maxAmountOfMessages) {
			boundedByAmountOfMessages = maxAmountOfMessages;
			return (BUILDER) this;
		}

		public BUILDER boundedByTimeSinceLastMessage(long timeSinceLastMessage) {
			boundedByTimeSinceLastMessage = timeSinceLastMessage;
			return (BUILDER) this;
		}

		private Bound<OUT> createBound() {
			if (boundedByAmountOfMessages != null && boundedByTimeSinceLastMessage != null) {
				return Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(boundedByAmountOfMessages, boundedByTimeSinceLastMessage);
			}

			if (boundedByAmountOfMessages != null) {
				return Bound.boundByAmountOfMessages(boundedByAmountOfMessages);
			}

			if (boundedByTimeSinceLastMessage != null) {
				return Bound.boundByTimeSinceLastMessage(boundedByTimeSinceLastMessage);
			}

			// This is functionally speaking no bound.
			return Bound.boundByAmountOfMessages(Long.MAX_VALUE);
		}

		@Override
		public PSS build() throws IOException {
			sourceUnderConstruction.setBound(createBound());
			return super.build();
		}
	}

}
