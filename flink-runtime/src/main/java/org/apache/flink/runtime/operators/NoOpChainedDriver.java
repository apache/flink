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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;

/**
 * A chained driver that just passes on the input as the output
 * @param <IT> The type of the input
 */
public class NoOpChainedDriver<IT> extends ChainedDriver<IT, IT> {

	@Override
	public void setup(AbstractInvokable parent) {

	}

	@Override
	public void openTask() throws Exception {

	}

	@Override
	public void closeTask() throws Exception {

	}

	@Override
	public void cancelTask() {

	}

	@Override
	public Function getStub() {
		return null;
	}

	@Override
	public String getTaskName() {
		return this.taskName;
	}

	@Override
	public void collect(IT record) {
		try {
			this.numRecordsIn.inc();
			this.outputCollector.collect(record);
		} catch (Exception ex) {
			throw new ExceptionInChainedStubException(this.taskName, ex);
		}
	}

	@Override
	public void close() {
		this.outputCollector.close();
	}
}
