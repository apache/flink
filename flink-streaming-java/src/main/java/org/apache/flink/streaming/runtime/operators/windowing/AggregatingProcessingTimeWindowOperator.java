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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;

@Internal
@Deprecated
public class AggregatingProcessingTimeWindowOperator<KEY, IN> 
		extends AbstractAlignedProcessingTimeWindowOperator<KEY, IN, IN, IN, ReduceFunction<IN>> {

	private static final long serialVersionUID = 7305948082830843475L;

	
	public AggregatingProcessingTimeWindowOperator(
			ReduceFunction<IN> function,
			KeySelector<IN, KEY> keySelector,
			TypeSerializer<KEY> keySerializer,
			TypeSerializer<IN> aggregateSerializer,
			long windowLength,
			long windowSlide)
	{
		super(function, keySelector, keySerializer, aggregateSerializer, windowLength, windowSlide);
	}

	@Override
	protected AggregatingKeyedTimePanes<IN, KEY> createPanes(KeySelector<IN, KEY> keySelector, Function function) {
		@SuppressWarnings("unchecked")
		ReduceFunction<IN> windowFunction = (ReduceFunction<IN>) function;
		
		return new AggregatingKeyedTimePanes<IN, KEY>(keySelector, windowFunction);
	}
}
