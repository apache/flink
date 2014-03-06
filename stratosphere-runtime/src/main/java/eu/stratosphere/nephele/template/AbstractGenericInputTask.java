/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.template;

import eu.stratosphere.core.io.GenericInputSplit;

/**
 * An input task that processes generic input splits (partitions).
 */
public abstract class AbstractGenericInputTask extends AbstractInputTask<GenericInputSplit> {


	@Override
	public GenericInputSplit[] computeInputSplits(final int requestedMinNumber) throws Exception {
		GenericInputSplit[] splits = new GenericInputSplit[requestedMinNumber];
		for (int i = 0; i < requestedMinNumber; i++) {
			splits[i] = new GenericInputSplit(i,requestedMinNumber);
		}
		return splits;
	}


	@Override
	public Class<GenericInputSplit> getInputSplitType() {

		return GenericInputSplit.class;
	}
}
