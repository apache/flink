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

package eu.stratosphere.nephele.streaming;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.util.StringUtils;

public final class TaskWrapper extends AbstractTask {

	static final String WRAPPED_CLASS_KEY = "streaming.class.name";

	private AbstractInvokable wrappedInvokable = null;

	private synchronized AbstractInvokable getWrappedInvokable() {

		if (this.wrappedInvokable != null) {
			return this.wrappedInvokable;
		}

		final Configuration conf = getEnvironment().getTaskConfiguration();
		final JobID jobID = getEnvironment().getJobID();
		final String className = conf.getString(WRAPPED_CLASS_KEY, null);
		if (className == null) {
			throw new IllegalStateException("Cannot find name of wrapped class");
		}

		try {
			final ClassLoader cl = LibraryCacheManager.getClassLoader(jobID);

			@SuppressWarnings("unchecked")
			final Class<? extends AbstractInvokable> invokableClass = (Class<? extends AbstractInvokable>) Class
				.forName(className, true, cl);

			this.wrappedInvokable = invokableClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(StringUtils.stringifyException(e));
		}

		this.wrappedInvokable.setEnvironment(new StreamingEnvironment(getEnvironment()));

		return this.wrappedInvokable;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		getWrappedInvokable().registerInputOutput();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		getWrappedInvokable().invoke();

	}
}
