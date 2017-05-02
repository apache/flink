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

import java.util.ArrayList;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UnionIterator;

/**
 * Key/value map organized in panes for accumulating windows (with a window function).
 */
@Internal
public class AccumulatingKeyedTimePanes<Type, Key, Result> extends AbstractKeyedTimePanes<Type, Key, ArrayList<Type>, Result> {

	private final KeySelector<Type, Key> keySelector;

	private final KeyMap.LazyFactory<ArrayList<Type>> listFactory = getListFactory();

	private final InternalWindowFunction<Iterable<Type>, Result, Key, Window> function;

	private final AccumulatingKeyedTimePanesContext context;

	/**
	 * IMPORTANT: This value needs to start at one, so it is fresher than the value that new entries
	 * have (zero).
	 */
	private long evaluationPass = 1L;

	// ------------------------------------------------------------------------

	public AccumulatingKeyedTimePanes(KeySelector<Type, Key> keySelector, InternalWindowFunction<Iterable<Type>, Result, Key, Window> function) {
		this.keySelector = keySelector;
		this.function = function;
		this.context = new AccumulatingKeyedTimePanesContext();
	}

	// ------------------------------------------------------------------------

	@Override
	public void addElementToLatestPane(Type element) throws Exception {
		Key k = keySelector.getKey(element);
		ArrayList<Type> elements = latestPane.putIfAbsent(k, listFactory);
		elements.add(element);
	}

	@Override
	public void evaluateWindow(Collector<Result> out, final TimeWindow window,
								AbstractStreamOperator<Result> operator) throws Exception {
		if (previousPanes.isEmpty()) {
			// optimized path for single pane case (tumbling window)
			for (KeyMap.Entry<Key, ArrayList<Type>> entry : latestPane) {
				Key key = entry.getKey();
				operator.setCurrentKey(key);
				context.globalState = operator.getKeyedStateStore();

				function.process(entry.getKey(), window, context, entry.getValue(), out);
			}
		}
		else {
			// general code path for multi-pane case
			WindowFunctionTraversal<Key, Type, Result> evaluator = new WindowFunctionTraversal<>(
					function, window, out, operator, context);
			traverseAllPanes(evaluator, evaluationPass);
		}

		evaluationPass++;
	}

	// ------------------------------------------------------------------------
	//  Running a window function in a map traversal
	// ------------------------------------------------------------------------

	static final class WindowFunctionTraversal<Key, Type, Result> implements KeyMap.TraversalEvaluator<Key, ArrayList<Type>> {

		private final InternalWindowFunction<Iterable<Type>, Result, Key, Window> function;

		private final UnionIterator<Type> unionIterator;

		private final Collector<Result> out;

		private final TimeWindow window;

		private final AbstractStreamOperator<Result> contextOperator;

		private Key currentKey;

		private AccumulatingKeyedTimePanesContext context;

		WindowFunctionTraversal(InternalWindowFunction<Iterable<Type>, Result, Key, Window> function, TimeWindow window,
								Collector<Result> out, AbstractStreamOperator<Result> contextOperator, AccumulatingKeyedTimePanesContext context) {
			this.function = function;
			this.out = out;
			this.unionIterator = new UnionIterator<>();
			this.window = window;
			this.contextOperator = contextOperator;
			this.context = context;
		}


		@Override
		public void startNewKey(Key key) {
			unionIterator.clear();
			currentKey = key;
		}

		@Override
		public void nextValue(ArrayList<Type> value) {
			unionIterator.addList(value);
		}

		@Override
		public void keyDone() throws Exception {
			contextOperator.setCurrentKey(currentKey);
			context.globalState = contextOperator.getKeyedStateStore();
			function.process(currentKey, window, context, unionIterator, out);
		}
	}

	// ------------------------------------------------------------------------
	//  Lazy factory for lists (put if absent)
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static <V> KeyMap.LazyFactory<ArrayList<V>> getListFactory() {
		return (KeyMap.LazyFactory<ArrayList<V>>) LIST_FACTORY;
	}

	private static class ThrowingKeyedStateStore implements KeyedStateStore {
		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not supported when using aligned processing-time windows.");
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not supported when using aligned processing-time windows.");
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not supported when using aligned processing-time windows.");
		}

		@Override
		public <T, A> FoldingState<T, A> getFoldingState(FoldingStateDescriptor<T, A> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not supported when using aligned processing-time windows.");
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not supported when using aligned processing-time windows.");
		}
	}

	private static class AccumulatingKeyedTimePanesContext implements InternalWindowFunction.InternalWindowContext {
		KeyedStateStore globalState;
		KeyedStateStore throwingStore;

		public AccumulatingKeyedTimePanesContext() {
			this.throwingStore = new ThrowingKeyedStateStore();
		}

		@Override
		public long currentProcessingTime() {
			throw new UnsupportedOperationException("current processing time is not supported in this context");
		}

		@Override
		public long currentWatermark() {
			throw new UnsupportedOperationException("current watermark is not supported in this context");
		}

		@Override
		public KeyedStateStore windowState() {
			return throwingStore;
		}

		@Override
		public KeyedStateStore globalState() {
			return globalState;
		}
	}

	private static final KeyMap.LazyFactory<?> LIST_FACTORY = new KeyMap.LazyFactory<ArrayList<?>>() {

		@Override
		public ArrayList<?> create() {
			return new ArrayList<>(4);
		}
	};
}
