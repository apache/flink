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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.siddhi.schema.StreamSchema;

/**
 * Wrap input event in generic type of <code>IN</code> as Tuple2<String,IN>
 */

public class StreamSiddhiOperator<IN,OUT> extends AbstractSiddhiOperator<Tuple2<String,IN>,OUT> {

	public StreamSiddhiOperator(SiddhiOperatorContext siddhiPlan) {
		super(siddhiPlan);
	}

	@Override
	protected void processEvent(String streamId, StreamSchema<Tuple2<String, IN>> schema, Tuple2<String, IN> value, long timestamp) throws InterruptedException {
		send(value.f0, getSiddhiContext().getInputStreamSchema(value.f0).getStreamSerializer().getRow(value.f1),timestamp);
	}

	@Override
	public String getStreamId(Tuple2<String, IN> record) {
		return record.f0;
	}
}
