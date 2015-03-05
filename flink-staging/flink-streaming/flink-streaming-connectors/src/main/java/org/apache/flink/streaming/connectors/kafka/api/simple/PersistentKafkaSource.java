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

package org.apache.flink.streaming.connectors.kafka.api.simple;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.streamvertex.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.util.DeserializationSchema;
import org.apache.flink.util.Collector;

public class PersistentKafkaSource<OUT> extends SimpleKafkaSource<OUT> {

	private static final long serialVersionUID = 1L;

	private long initialOffset;

	private transient OperatorState<Long> kafkaOffSet;

	/**
	 * Partition index is set automatically by instance id.
	 * 
	 * @param topicId
	 * @param host
	 * @param port
	 * @param deserializationSchema
	 */
	public PersistentKafkaSource(String topicId, String host, int port, long initialOffset,
			DeserializationSchema<OUT> deserializationSchema) {
		super(topicId, host, port, deserializationSchema);
		this.initialOffset = initialOffset;
	}

	@Override
	public void open(Configuration parameters) {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		@SuppressWarnings("unchecked")
		OperatorState<Long> lastKafkaOffSet = (OperatorState<Long>) context.getState("kafka");

		if (lastKafkaOffSet.getState() == null) {
			kafkaOffSet = new OperatorState<Long>(initialOffset);
			context.registerState("kafka", kafkaOffSet);
		} else {
			kafkaOffSet = lastKafkaOffSet;
		}

		super.open(parameters);
	}

	@Override
	protected void setInitialOffset(Configuration config) {
		iterator.initializeFromOffset(kafkaOffSet.getState());
	}

	@Override
	protected void gotMessage(MessageWithOffset msg) {
		System.out.println(msg.getOffset() + " :: " + schema.deserialize(msg.getMessage()));
	}

	@Override
	public void run(Collector<OUT> collector) throws Exception {
		MessageWithOffset msg;
		while (iterator.hasNext()) {
			msg = iterator.nextWithOffset();
			gotMessage(msg);
			OUT out = schema.deserialize(msg.getMessage());
			collector.collect(out);
			kafkaOffSet.update(msg.getOffset());
		}
	}
}
