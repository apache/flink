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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * The base class for substitute stream operators with two inputs.
 *
 * @param <IN1> The input type of the actual operator
 * @param <IN2> The input type of the actual operator
 * @param <OUT> The output type of the actual operator
 */
public interface AbstractTwoInputSubstituteStreamOperator<IN1, IN2, OUT> extends AbstractSubstituteStreamOperator<OUT>,
	TwoInputStreamOperator<IN1, IN2, OUT> {

	@Override
	default TwoInputSelection firstInputSelection() {
		throw new UnsupportedOperationException("For an AbstractTwoInputSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default TwoInputSelection processElement1(StreamRecord<IN1> element) throws Exception {
		throw new UnsupportedOperationException("For an AbstractTwoInputSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default TwoInputSelection processElement2(StreamRecord<IN2> element) throws Exception {
		throw new UnsupportedOperationException("For an AbstractTwoInputSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void processWatermark1(Watermark mark) throws Exception {
		throw new UnsupportedOperationException("For an AbstractTwoInputSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void processWatermark2(Watermark mark) throws Exception {
		throw new UnsupportedOperationException("For an AbstractTwoInputSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
		throw new UnsupportedOperationException("For an AbstractTwoInputSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
		throw new UnsupportedOperationException("For an AbstractTwoInputSubstituteStreamOperator, this method should not be called");
	}
}
