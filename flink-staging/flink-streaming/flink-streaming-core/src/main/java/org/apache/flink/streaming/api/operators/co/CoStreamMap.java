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

import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class CoStreamMap<IN1, IN2, OUT> extends CoStreamOperator<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private CoMapFunction<IN1, IN2, OUT> mapper;

	public CoStreamMap(CoMapFunction<IN1, IN2, OUT> mapper) {
		super(mapper);
		this.mapper = mapper;
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
	protected void callUserFunction1() throws Exception {
		collector.collect(mapper.map1(reuse1.getObject()));

	}

	@Override
	protected void callUserFunction2() throws Exception {
		collector.collect(mapper.map2(reuse2.getObject()));

	}

}
