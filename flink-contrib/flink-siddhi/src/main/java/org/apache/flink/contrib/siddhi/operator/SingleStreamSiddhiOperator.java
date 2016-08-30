/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.operator;

import org.apache.flink.contrib.siddhi.schema.StreamSchema;
import org.apache.flink.contrib.siddhi.schema.StreamSerializer;
import org.wso2.siddhi.core.stream.input.InputHandler;

/**
 * Pass through event for single stream siddhi query in consideration of higher performance.
 */
public class SingleStreamSiddhiOperator<IN,OUT> extends AbstractSiddhiOperator<IN,OUT> {
	private final String inputStreamId;
	private final StreamSerializer<IN> inStreamSerializer;

	private transient InputHandler inputHandler;

	@SuppressWarnings("unchecked")
	public SingleStreamSiddhiOperator(String streamId, SiddhiOperatorContext siddhiPlan) {
		super(siddhiPlan);
		assert siddhiPlan.getInputStreams().size() == 1;
		this.inputStreamId = streamId;
		this.inStreamSerializer = siddhiPlan.<IN>getInputStreamSchema(this.inputStreamId).getStreamSerializer();
	}

	@Override
	protected void startSiddhiRuntime() {
		super.startSiddhiRuntime();
		inputHandler = this.getSiddhiInputHandler(inputStreamId);
	}

	@Override
	protected void shutdownSiddhiRuntime() {
		super.shutdownSiddhiRuntime();
		inputHandler = null;
	}

	@Override
	protected void processEvent(String streamId, StreamSchema<IN> schema, IN value, long timestamp) throws Exception {
		Object[] row = this.inStreamSerializer.getRow(value);
		inputHandler.send(timestamp,row);
	}

	@Override
	public String getStreamId(Object record) {
		return this.inputStreamId;
	}
}
