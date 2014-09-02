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


package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.GenericCollectorMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.RegularPactTask;

public class ChainedCollectorMapDriver<IT, OT> extends ChainedDriver<IT, OT> {

	private GenericCollectorMap<IT, OT> mapper;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		@SuppressWarnings("unchecked")
		final GenericCollectorMap<IT, OT> mapper =
			RegularPactTask.instantiateUserCode(this.config, userCodeClassLoader, GenericCollectorMap.class);
		this.mapper = mapper;
		mapper.setRuntimeContext(getUdfRuntimeContext());
	}

	@Override
	public void openTask() throws Exception {
		Configuration stubConfig = this.config.getStubParameters();
		RegularPactTask.openUserCode(this.mapper, stubConfig);
	}

	@Override
	public void closeTask() throws Exception {
		RegularPactTask.closeUserCode(this.mapper);
	}

	@Override
	public void cancelTask() {
		try {
			this.mapper.close();
		} catch (Throwable t) {
		}
	}

	// --------------------------------------------------------------------------------------------

	public RichFunction getStub() {
		return this.mapper;
	}

	public String getTaskName() {
		return this.taskName;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void collect(IT record) {
		try {
			this.mapper.map(record, this.outputCollector);
		} catch (Exception ex) {
			throw new ExceptionInChainedStubException(this.taskName, ex);
		}
	}

	@Override
	public void close() {
		this.outputCollector.close();
	}
}
