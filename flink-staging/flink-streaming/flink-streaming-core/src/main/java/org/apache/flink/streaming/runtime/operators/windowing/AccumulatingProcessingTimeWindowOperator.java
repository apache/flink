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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windows.KeyedWindowFunction;


public class AccumulatingProcessingTimeWindowOperator<KEY, IN, OUT> 
		extends AbstractAlignedProcessingTimeWindowOperator<KEY, IN, OUT>  {

	private static final long serialVersionUID = 7305948082830843475L;

	
	public AccumulatingProcessingTimeWindowOperator(
			KeyedWindowFunction<IN, OUT, KEY> function,
			KeySelector<IN, KEY> keySelector,
			long windowLength,
			long windowSlide)
	{
		super(function, keySelector, windowLength, windowSlide);
	}

	@Override
	protected AccumulatingKeyedTimePanes<IN, KEY, OUT> createPanes(KeySelector<IN, KEY> keySelector, Function function) {
		@SuppressWarnings("unchecked")
		KeyedWindowFunction<IN, OUT, KEY> windowFunction = (KeyedWindowFunction<IN, OUT, KEY>) function;
		
		return new AccumulatingKeyedTimePanes<>(keySelector, windowFunction);
	}
}
