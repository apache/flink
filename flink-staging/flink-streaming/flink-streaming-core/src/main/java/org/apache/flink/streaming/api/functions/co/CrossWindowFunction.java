/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.co;

import java.util.List;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.util.Collector;

public class CrossWindowFunction<IN1, IN2, OUT> implements CoWindowFunction<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private CrossFunction<IN1, IN2, OUT> crossFunction;

	public CrossWindowFunction(CrossFunction<IN1, IN2, OUT> crossFunction) {
		this.crossFunction = crossFunction;
	}

	@Override
	public void coWindow(List<IN1> first, List<IN2> second, Collector<OUT> out) throws Exception {
		for (IN1 firstValue : first) {
			for (IN2 secondValue : second) {
				out.collect(crossFunction.cross(firstValue, secondValue));
			}
		}
	}

}