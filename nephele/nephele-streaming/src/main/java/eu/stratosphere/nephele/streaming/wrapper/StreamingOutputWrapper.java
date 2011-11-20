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

package eu.stratosphere.nephele.streaming.wrapper;

import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractOutputTask;

/**
 * This class provides a wrapper for Nephele tasks of the type {@link AbstractOutputTask}.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class StreamingOutputWrapper extends AbstractOutputTask {

	private volatile AbstractInvokable wrappedInvokable = null;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		this.wrappedInvokable = WrapperUtils.getWrappedInvokable(getEnvironment());
		this.wrappedInvokable.registerInputOutput();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		this.wrappedInvokable.invoke();
	}
}
