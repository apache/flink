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
package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainedAllReduceDriver<IT> extends ChainedDriver<IT, IT> {
	private static final Logger LOG = LoggerFactory.getLogger(ChainedAllReduceDriver.class);

	// --------------------------------------------------------------------------------------------
	private ReduceFunction<IT> reducer;
	private TypeSerializer<IT> serializer;

	private IT base;

	// --------------------------------------------------------------------------------------------
	@Override
	public void setup(AbstractInvokable parent) {
		final ReduceFunction<IT> red = BatchTask.instantiateUserCode(this.config, userCodeClassLoader, ReduceFunction.class);
		this.reducer = red;
		FunctionUtils.setFunctionRuntimeContext(red, getUdfRuntimeContext());

		TypeSerializerFactory<IT> serializerFactory = this.config.getInputSerializer(0, userCodeClassLoader);
		this.serializer = serializerFactory.getSerializer();

		if (LOG.isDebugEnabled()) {
			LOG.debug("ChainedAllReduceDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void openTask() throws Exception {
		Configuration stubConfig = this.config.getStubParameters();
		BatchTask.openUserCode(this.reducer, stubConfig);
	}

	@Override
	public void closeTask() throws Exception {
		BatchTask.closeUserCode(this.reducer);
	}

	@Override
	public void cancelTask() {
		try {
			FunctionUtils.closeFunction(this.reducer);
		} catch (Throwable t) {
			// Ignore exception.
		}
	}

	// --------------------------------------------------------------------------------------------
	@Override
	public Function getStub() {
		return this.reducer;
	}

	@Override
	public String getTaskName() {
		return this.taskName;
	}

	// --------------------------------------------------------------------------------------------
	@Override
	public void collect(IT record) {
		numRecordsIn.inc();
		try {
			if (base == null) {
				base = serializer.copy(record);
			} else {
				base = objectReuseEnabled ? reducer.reduce(base, record) : serializer.copy(reducer.reduce(base, record));
			}
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(taskName, e);
		}
	}

	@Override
	public void close() {
		try {
			if (base != null) {
				this.outputCollector.collect(base);
				base = null;
			}
		} catch (Exception e) {
			throw new ExceptionInChainedStubException(this.taskName, e);
		}
		this.outputCollector.close();
	}
}
