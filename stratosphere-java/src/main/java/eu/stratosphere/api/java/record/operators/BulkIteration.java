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

package eu.stratosphere.api.java.record.operators;

import eu.stratosphere.api.common.operators.base.BulkIterationBase;
import eu.stratosphere.types.Record;

/**
 * 
 */
public class BulkIteration extends BulkIterationBase<Record> {
	public BulkIteration() {
		super(OperatorInfoHelper.unary());
	}

	public BulkIteration(String name) {
		super(OperatorInfoHelper.unary(), name);
	}

	/**
	 * Specialized operator to use as a recognizable place-holder for the input to the
	 * step function when composing the nested data flow.
	 */
	public static class PartialSolutionPlaceHolder extends BulkIterationBase.PartialSolutionPlaceHolder<Record> {
		public PartialSolutionPlaceHolder(BulkIterationBase<Record> container) {
			super(container, OperatorInfoHelper.unary());
		}
	}
}
