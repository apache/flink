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

import eu.stratosphere.api.common.operators.OperatorInformation;
import eu.stratosphere.api.common.operators.base.DeltaIterationBase;
import eu.stratosphere.api.java.typeutils.RecordTypeInfo;
import eu.stratosphere.types.Record;

/**
 * A DeltaIteration is similar to a {@link BulkIteration}, 
 * but maintains state across the individual iteration steps. The state is called the <i>solution set</i>, can be obtained
 * via {@link #getSolutionSet()}, and be accessed by joining (or CoGrouping) with it. The solution
 * set is updated by producing a delta for it, which is merged into the solution set at the end of each iteration step.
 * <p>
 * The delta iteration must be closed by setting a delta for the solution set ({@link #setSolutionSetDelta(eu.stratosphere.api.common.operators.Operator)})
 * and the new workset (the data set that will be fed back, {@link #setNextWorkset(eu.stratosphere.api.common.operators.Operator)}).
 * The DeltaIteration itself represents the result after the iteration has terminated.
 * Delta iterations terminate when the feed back data set (the workset) is empty.
 * In addition, a maximum number of steps is given as a fall back termination guard.
 * <p>
 * Elements in the solution set are uniquely identified by a key. When merging the solution set delta, contained elements
 * with the same key are replaced.
 * <p>
 * This class is a subclass of {@code DualInputOperator}. The solution set is considered the first input, the
 * workset is considered the second input.
 */
public class DeltaIteration extends DeltaIterationBase<Record, Record> {

	public DeltaIteration(int keyPosition) {
		super(OperatorInfoHelper.binary(), keyPosition);
	}
	
	public DeltaIteration(int[] keyPositions) {
		super(OperatorInfoHelper.binary(), keyPositions);
	}

	public DeltaIteration(int keyPosition, String name) {
		super(OperatorInfoHelper.binary(), keyPosition, name);
	}
	
	public DeltaIteration(int[] keyPositions, String name) {
		super(OperatorInfoHelper.binary(), keyPositions, name);
	}

	/**
	 * Specialized operator to use as a recognizable place-holder for the working set input to the
	 * step function.
	 */
	public static class WorksetPlaceHolder extends DeltaIterationBase.WorksetPlaceHolder<Record> {
		public WorksetPlaceHolder(DeltaIterationBase<?, Record> container) {
			super(container, new OperatorInformation<Record>(new RecordTypeInfo()));
		}
	}

	/**
	 * Specialized operator to use as a recognizable place-holder for the solution set input to the
	 * step function.
	 */
	public static class SolutionSetPlaceHolder extends DeltaIterationBase.SolutionSetPlaceHolder<Record> {
		public SolutionSetPlaceHolder(DeltaIterationBase<Record, ?> container) {
			super(container, new OperatorInformation<Record>(new RecordTypeInfo()));
		}
	}
}
