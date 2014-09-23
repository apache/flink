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


package org.apache.flink.runtime.operators;

//public class UnionWithTempOperator<T> implements PactDriver<Function, T> {
//	
//	private PactTaskContext<Function, T> taskContext;
//	
//	private volatile boolean running;
//	
//	
//	@Override
//	public void setup(PactTaskContext<Function, T> context) {
//		this.taskContext = context;
//		this.running = true;
//	}
//
//	@Override
//	public int getNumberOfInputs() {
//		return 2;
//	}
//
//	@Override
//	public Class<Function> getStubType() {
//		return Function.class;
//	}
//
//	@Override
//	public boolean requiresComparatorOnInput() {
//		return false;
//	}
//
//	@Override
//	public void prepare() {}
//
//	@Override
//	public void run() throws Exception {
//		
//		final int tempedInput = 0;
//		final int streamedInput = 1;
//		
//		final MutableObjectIterator<T> cache = this.taskContext.getInput(tempedInput);
//		final MutableObjectIterator<T> input = this.taskContext.getInput(streamedInput);
//		
//		final Collector<T> output = this.taskContext.getOutputCollector();
//
//		T record = this.taskContext.<T>getInputSerializer(streamedInput).createInstance();
//
//		while (this.running && ((record = input.next(record)) != null)) {
//			output.collect(record);
//		}
//		while (this.running && ((record = cache.next(record)) != null)) {
//			output.collect(record);
//		}
//	}
//
//	@Override
//	public void cleanup() {}
//
//	@Override
//	public void cancel() {
//		this.running = false;
//	}
//}
