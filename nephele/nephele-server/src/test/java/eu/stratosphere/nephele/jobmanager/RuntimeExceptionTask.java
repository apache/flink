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

package eu.stratosphere.nephele.jobmanager;

import eu.stratosphere.nephele.template.AbstractTask;

/**
 * This task throws a {@link RuntimeException} when the method <code>registerInputOutput</code> is called.
 * 
 * @author warneke
 */
public class RuntimeExceptionTask extends AbstractTask {

	/**
	 * The message which is used for the test runtime exception.
	 */
	public static final String RUNTIME_EXCEPTION_MESSAGE = "This is a test runtime exception";

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		throw new RuntimeException(RUNTIME_EXCEPTION_MESSAGE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		// Nothing to do here
	}

}
