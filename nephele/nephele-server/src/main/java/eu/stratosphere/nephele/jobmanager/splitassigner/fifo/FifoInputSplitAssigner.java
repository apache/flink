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

package eu.stratosphere.nephele.jobmanager.splitassigner.fifo;

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitAssigner;
import eu.stratosphere.nephele.template.InputSplit;

/**
 * The independent input split assigner is a specific implementation of the {@link InputSplitAssigner} interface for
 * {@link InputSplit} objects can be location independently scheduled. 
 * <p>
 * This class is thread-safe.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public final class FifoInputSplitAssigner implements InputSplitAssigner {

	/**
	 * The logging object which is used to report information and errors.
	 */
	private static final Log LOG = LogFactory.getLog(FifoInputSplitAssigner.class);

	private final LinkedList<InputSplit> inputSplits = new LinkedList<InputSplit>();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerGroupVertex(final ExecutionGroupVertex groupVertex) {

		// Collect all input splits in a linked list
		for(InputSplit s : groupVertex.getInputSplits()) {
			this.inputSplits.add(s);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterGroupVertex(final ExecutionGroupVertex groupVertex) {
		// nothing to be done here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit getNextInputSplit(final ExecutionVertex vertex) {

		return this.inputSplits.poll();
	}

}
