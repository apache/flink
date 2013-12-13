/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.chaining;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.generic.stub.GenericMapper;
import eu.stratosphere.pact.runtime.task.RegularPactTask;

public class ChainedMapDriver<IT, OT> extends ChainedDriver<IT, OT> {

	private GenericMapper<IT, OT> mapper;

	// --------------------------------------------------------------------------------------------

	@Override
	public void setup(AbstractInvokable parent) {
		@SuppressWarnings("unchecked")
		final GenericMapper<IT, OT> mapper =
			RegularPactTask.instantiateUserCode(this.config, userCodeClassLoader, GenericMapper.class);
		this.mapper = mapper;
		mapper.setRuntimeContext(getRuntimeContext(parent, this.taskName));
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

	public Stub getStub() {
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
