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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.RepartitionTaskEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Partitioner that distributes the data equally by cycling through the output
 * channels.
 *
 * @param <T> Type of the elements in the Stream being rebalanced
 */
@Internal
public class DynamicKeyedFixedPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	protected static final Logger LOG = LoggerFactory.getLogger(DynamicKeyedFixedPartitioner.class);

	private static final long serialVersionUID = 1L;

	//	private int nextChannelToSendTo;
	private int nextChannelToSendTo = 0;

	private int maxParallelism = 0;

	private final KeySelector<T, K> keySelector;

	protected final Random rng = new XORShiftRandom();

	public DynamicKeyedFixedPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
	}

	@Override
	public void setup(int numberOfChannels) {
		super.setup(numberOfChannels);
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		return nextChannelToSendTo;
	}

	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "DYNAMICKEYEDFIXED";
	}

	@Override
	public void handleEvent(AbstractEvent event) {
		nextChannelToSendTo = ((RepartitionTaskEvent)event).getInteger();
		LOG.debug("DynamicKeyedFixedPartitioner handleEvent RepartitionTaskEvent {}", nextChannelToSendTo);
	}
	/**
	 * Configure the {@link StreamPartitioner} with the maximum parallelism of the down stream
	 * operator.
	 *
	 * @param maxParallelism Maximum parallelism of the down stream operator.
	 */
	@Override
	public void configure(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}
}
