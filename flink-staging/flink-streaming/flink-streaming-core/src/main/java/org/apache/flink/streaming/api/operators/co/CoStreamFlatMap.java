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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;

public class CoStreamFlatMap<IN1, IN2, OUT> extends CoStreamOperator<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	public CoStreamFlatMap(CoFlatMapFunction<IN1, IN2, OUT> flatMapper) {
		super(flatMapper);
	}

	@Override
	public void handleStream1() throws Exception {
		callUserFunctionAndLogException1();
	}

	@Override
	public void handleStream2() throws Exception {
		callUserFunctionAndLogException2();
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void callUserFunction1() throws Exception {
		((CoFlatMapFunction<IN1, IN2, OUT>) userFunction).flatMap1(reuse1.getObject(), collector);

	}

	@Override
	@SuppressWarnings("unchecked")
	protected void callUserFunction2() throws Exception {
		((CoFlatMapFunction<IN1, IN2, OUT>) userFunction).flatMap2(reuse2.getObject(), collector);

	}

}
