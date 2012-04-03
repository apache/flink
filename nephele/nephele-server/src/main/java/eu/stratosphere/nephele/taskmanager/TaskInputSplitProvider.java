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

package eu.stratosphere.nephele.taskmanager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitWrapper;
import eu.stratosphere.nephele.protocols.InputSplitProviderProtocol;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The task input split provider is a component of the task manager which implements the {@link InputSplitProvider}
 * interface. It is called by a task in order to acquire a new input split to consume. The task input split provider in
 * return will call the global input split provider to retrieve a new input split.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class TaskInputSplitProvider implements InputSplitProvider {

	private final JobID jobID;

	private final ExecutionVertexID executionVertexID;

	private final InputSplitProviderProtocol globalInputSplitProvider;

	private final AtomicInteger sequenceNumber = new AtomicInteger(0);

	TaskInputSplitProvider(final JobID jobID, final ExecutionVertexID executionVertexID,
			final InputSplitProviderProtocol globalInputSplitProvider) {

		this.jobID = jobID;
		this.executionVertexID = executionVertexID;
		this.globalInputSplitProvider = globalInputSplitProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit getNextInputSplit() {

		try {

			synchronized (this.globalInputSplitProvider) {
				final InputSplitWrapper wrapper = this.globalInputSplitProvider.requestNextInputSplit(this.jobID,
					this.executionVertexID, new IntegerRecord(this.sequenceNumber.getAndIncrement()));
				return wrapper.getInputSplit();
			}

		} catch (IOException ioe) {
			// Convert IOException into a RuntimException and let the regular fault tolerance routines take care of the
			// rest
			throw new RuntimeException(StringUtils.stringifyException(ioe));
		}
	}
}
