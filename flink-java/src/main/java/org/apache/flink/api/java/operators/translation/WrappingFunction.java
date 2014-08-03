/**
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

package org.apache.flink.api.java.operators.translation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Value;


public abstract class WrappingFunction<T extends Function> extends AbstractRichFunction {
	
	private static final long serialVersionUID = 1L;

	protected T wrappedFunction;

	protected WrappingFunction(T wrappedFunction) {
		this.wrappedFunction = wrappedFunction;
	}

	
	@Override
	public void open(Configuration parameters) throws Exception {
		FunctionUtils.openFunction(this.wrappedFunction, parameters);
	}
	
	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(this.wrappedFunction);
	}
	
	@Override
	public void setRuntimeContext(RuntimeContext t) {
		super.setRuntimeContext(t);
		
		if (t instanceof IterationRuntimeContext) {
			FunctionUtils.setFunctionRuntimeContext(this.wrappedFunction, new WrappingIterationRuntimeContext(t));
		}
		else{
			FunctionUtils.setFunctionRuntimeContext(this.wrappedFunction, new WrappingRuntimeContext(t));
		}
	}

	public T getWrappedFunction () {
		return this.wrappedFunction;
	}
	
	
	private static class WrappingRuntimeContext implements RuntimeContext {

		protected final RuntimeContext context;
		
		
		protected WrappingRuntimeContext(RuntimeContext context) {
			this.context = context;
		}

		@Override
		public String getTaskName() {
			return context.getTaskName();
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return context.getNumberOfParallelSubtasks();
		}

		@Override
		public int getIndexOfThisSubtask() {
			return context.getIndexOfThisSubtask();
		}


		@Override
		public <V, A> void addAccumulator(String name, Accumulator<V, A> accumulator) {
			context.<V, A>addAccumulator(name, accumulator);
		}

		@Override
		public <V, A> Accumulator<V, A> getAccumulator(String name) {
			return context.<V, A>getAccumulator(name);
		}

		@Override
		public HashMap<String, Accumulator<?, ?>> getAllAccumulators() {
			return context.getAllAccumulators();
		}

		@Override
		public IntCounter getIntCounter(String name) {
			return context.getIntCounter(name);
		}

		@Override
		public LongCounter getLongCounter(String name) {
			return context.getLongCounter(name);
		}

		@Override
		public DoubleCounter getDoubleCounter(String name) {
			return context.getDoubleCounter(name);
		}

		@Override
		public Histogram getHistogram(String name) {
			return context.getHistogram(name);
		}

		@Override
		public <RT> Collection<RT> getBroadcastVariable(String name) {
			Collection<RT> refColl = context.getBroadcastVariable(name);
			
			ArrayList<RT> list = new ArrayList<RT>(refColl.size());
			for (RT e : refColl) {
				list.add(e);
			}
			
			return list;
		}

		@Override
		public DistributedCache getDistributedCache() {
			return context.getDistributedCache();
		}
	}
	
	private static class WrappingIterationRuntimeContext extends WrappingRuntimeContext implements IterationRuntimeContext {

		protected WrappingIterationRuntimeContext(RuntimeContext context) {
			super(context);
		}

		@Override
		public int getSuperstepNumber() {
			return ((IterationRuntimeContext) context).getSuperstepNumber();
		}

		@Override
		public <T extends Aggregator<?>> T getIterationAggregator(String name) {
			return ((IterationRuntimeContext) context).<T>getIterationAggregator(name);
		}

		@Override
		public <T extends Value> T getPreviousIterationAggregate(String name) {
			return ((IterationRuntimeContext) context).<T>getPreviousIterationAggregate(name);
		}
	}
}
