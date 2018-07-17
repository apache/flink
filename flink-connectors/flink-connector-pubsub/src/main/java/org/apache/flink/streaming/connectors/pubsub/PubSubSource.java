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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them as soon as they have been received.
 */
public class PubSubSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, AckReplyConsumer> implements MessageReceiver, ResultTypeQueryable<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);

	private final DeserializationSchema<OUT> deserializationSchema;
	private final SubscriberWrapper          subscriberWrapper;
	private final PubSubSourceBuilder.Mode   mode;
	private final boolean                    autoAcknowledge;

	protected transient SourceContext<OUT> sourceContext = null;

	PubSubSource(SubscriberWrapper subscriberWrapper, DeserializationSchema<OUT> deserializationSchema, PubSubSourceBuilder.Mode mode) {
		super(String.class);
		this.deserializationSchema = deserializationSchema;
		this.subscriberWrapper     = subscriberWrapper;
		this.mode                  = mode;
		this.autoAcknowledge       = mode == PubSubSourceBuilder.Mode.NONE;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		subscriberWrapper.initialize(this);
		if (hasNoCheckpointingEnabled(getRuntimeContext()) && needsCheckpointing()) {
			throw new IllegalArgumentException("Checkpointing needs to be enabled to support: " + mode);
		}
	}

	private boolean hasNoCheckpointingEnabled(RuntimeContext runtimeContext) {
		return !(runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled());
	}

	@Override
	protected void acknowledgeSessionIDs(List<AckReplyConsumer> ackReplyConsumers) {
		ackReplyConsumers.forEach(AckReplyConsumer::ack);
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) {
		this.sourceContext = sourceContext;
		subscriberWrapper.startBlocking();
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		if (sourceContext == null) {
			consumer.nack();
			return;
		}

		if (autoAcknowledge) {
			sourceContext.collect(deserializeMessage(message));
			consumer.ack();
			return;
		}

		processMessage(message, consumer);
	}

	private void processMessage(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
		synchronized (sourceContext.getCheckpointLock()) {
			boolean alreadyProcessed = !addId(message.getMessageId());
			if (alreadyProcessed) {
				return;
			}

			sessionIds.add(ackReplyConsumer);
			sourceContext.collect(deserializeMessage(message));
		}
	}

	@Override
	public void cancel() {
		subscriberWrapper.stop();
	}

	private OUT deserializeMessage(PubsubMessage message) {
		try {
			return deserializationSchema.deserialize(message.getData().toByteArray());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deserializationSchema.getProducedType();
	}

	private boolean needsCheckpointing() {
		return mode == PubSubSourceBuilder.Mode.ATLEAST_ONCE || mode == PubSubSourceBuilder.Mode.EXACTLY_ONCE;
	}
}
