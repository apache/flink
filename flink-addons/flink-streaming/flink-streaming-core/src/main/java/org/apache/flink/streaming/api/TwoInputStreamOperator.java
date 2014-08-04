/**
 *
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
 *
 */

package org.apache.flink.streaming.api;

/**
 * The TwoInputStreamOperator represents a {@link StreamOperator} with two
 * inputs.
 *
 * @param <IN1>
 *            Type of the first input.
 * 
 * @param <IN2>
 *            Type of the second input.
 * @param <OUT>
 *            Output Type of the operator.
 */
public class TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT> {

	protected TwoInputStreamOperator(StreamExecutionEnvironment environment, String operatorType) {
		super(environment, operatorType);
	}

	protected TwoInputStreamOperator(DataStream<OUT> dataStream) {
		super(dataStream);
	}

	@Override
	protected DataStream<OUT> copy() {
		return new TwoInputStreamOperator<IN1, IN2, OUT>(this);
	}

}
