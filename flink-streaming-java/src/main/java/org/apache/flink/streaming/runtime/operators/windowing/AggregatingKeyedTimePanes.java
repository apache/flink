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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@Internal
public class AggregatingKeyedTimePanes<Type, Key> extends AbstractKeyedTimePanes<Type, Key, Type, Type> {
	
	private final KeySelector<Type, Key> keySelector;
	
	private final ReduceFunction<Type> reducer;

	/**
	 * IMPORTANT: This value needs to start at one, so it is fresher than the value that new entries have (zero) */
	private long evaluationPass = 1L;

	// ------------------------------------------------------------------------
	
	public AggregatingKeyedTimePanes(KeySelector<Type, Key> keySelector, ReduceFunction<Type> reducer) {
		this.keySelector = keySelector;
		this.reducer = reducer;
	}

	// ------------------------------------------------------------------------

	@Override
	public void addElementToLatestPane(Type element) throws Exception {
		Key k = keySelector.getKey(element);
		latestPane.putOrAggregate(k, element, reducer);
	}

	@Override
	public void evaluateWindow(Collector<Type> out, TimeWindow window, 
								AbstractStreamOperator<Type> operator) throws Exception {
		if (previousPanes.isEmpty()) {
			// optimized path for single pane case
			for (KeyMap.Entry<Key, Type> entry : latestPane) {
				out.collect(entry.getValue());
			}
		}
		else {
			// general code path for multi-pane case
			AggregatingTraversal<Key, Type> evaluator = new AggregatingTraversal<>(reducer, out, operator);
			traverseAllPanes(evaluator, evaluationPass);
		}
		
		evaluationPass++;
	}

	// ------------------------------------------------------------------------
	//  The maps traversal that performs the final aggregation
	// ------------------------------------------------------------------------
	
	static final class AggregatingTraversal<Key, Type> implements KeyMap.TraversalEvaluator<Key, Type> {

		private final ReduceFunction<Type> function;
		
		private final Collector<Type> out;
		
		private final AbstractStreamOperator<Type> operator;
		
		private Type currentValue;

		AggregatingTraversal(ReduceFunction<Type> function, Collector<Type> out,
								AbstractStreamOperator<Type> operator) {
			this.function = function;
			this.out = out;
			this.operator = operator;
		}

		@Override
		public void startNewKey(Key key) {
			currentValue = null;
			operator.setCurrentKey(key);
		}

		@Override
		public void nextValue(Type value) throws Exception {
			if (currentValue != null) {
				currentValue = function.reduce(currentValue, value);
			}
			else {
				currentValue = value;
			}
		}

		@Override
		public void keyDone() throws Exception {
			out.collect(currentValue);
		}
	}
}
