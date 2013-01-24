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

package eu.stratosphere.pact.generic.io;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.generic.io.InputFormat;

/**
 * Generic base class for all inputs that are not based on files. 
 *
 * @author Stephan Ewen
 */
public abstract class GenericInputFormat<OT> implements InputFormat<OT, GenericInputSplit>
{	
	/**
	 * The partition of this split.
	 */
	protected int partitionNumber;

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters) {
		//	nothing by default
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		// no statistics available, by default.
		return cachedStatistics;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#createInputSplits(int)
	 */
	@Override
	public GenericInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
		for (int i = 0; i < splits.length; i++) {
			splits[i] = new GenericInputSplit(i);
		}
		return splits;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getInputSplitType()
	 */
	@Override
	public Class<? extends GenericInputSplit> getInputSplitType() {
		return GenericInputSplit.class;
	}

	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#open(eu.stratosphere.nephele.template.InputSplit)
	 */
	@Override
	public void open(GenericInputSplit split) throws IOException {
		this.partitionNumber = split.getSplitNumber();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#close()
	 */
	@Override
	public void close() throws IOException {
		// nothing by default 	
	}
}
