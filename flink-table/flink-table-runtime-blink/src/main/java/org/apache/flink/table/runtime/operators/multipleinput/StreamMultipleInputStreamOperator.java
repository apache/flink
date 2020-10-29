/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.StateNameAware;
import org.apache.flink.table.runtime.operators.StateNameAwareStreamOperator;
import org.apache.flink.table.runtime.operators.StateNameContext;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;

import java.util.Iterator;
import java.util.List;

/**
 * A {@link MultipleInputStreamOperatorBase} to handle streaming operators.
 */
public class StreamMultipleInputStreamOperator extends MultipleInputStreamOperatorBase {

	private static final long serialVersionUID = 1L;

	public StreamMultipleInputStreamOperator(
			StreamOperatorParameters<RowData> parameters,
			List<InputSpec> inputSpecs,
			List<TableOperatorWrapper<?>> headWrappers,
			TableOperatorWrapper<?> tailWrapper) {
		super(parameters, inputSpecs, headWrappers, tailWrapper);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open() throws Exception {
		// initializeState for each operator first
		StateNameContext stateNameContext = new StateNameContext();
		final Iterator<TableOperatorWrapper<?>> it = topologicalOrderingOperators.descendingIterator();
		while (it.hasNext()) {
			StreamOperator<?> operator = it.next().getStreamOperator();
			if (operator instanceof AbstractUdfStreamOperator) {
				Function function = ((AbstractUdfStreamOperator) operator).getUserFunction();
				if (function instanceof StateNameAware) {
					((StateNameAware) function).setStateNameContext(stateNameContext);
				} else {
					throw new RuntimeException(
							function.getClass().getName() + " must extend from StateNameAware to make sure " +
									"all state names in a multiple input operator are unique " +
									"(via getStateNameContext().getUniqueStateName(\"your_state_name\")).\n" +
									"The example is StateNameAwareKeyedProcessFunction.");
				}
			} else if (operator instanceof StateNameAwareStreamOperator) {
				((StateNameAware) operator).setStateNameContext(stateNameContext);
			} else {
				throw new RuntimeException(
						operator.getClass().getName() + " must extend from StateNameAware to make sure " +
								"all state names in a multiple input operator are unique " +
								"(via getStateNameContext().getUniqueStateName(\"your_state_name\")).\n" +
								"The example is StateNameAwareStreamOperator.");
			}

			// initialize state
			((AbstractStreamOperator<?>) operator).initializeState(
					getStateHandler().orElseThrow(() -> new RuntimeException("This should not happen.")),
					getTimeServiceManager().orElse(null));
		}
		super.open();
	}

	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		super.prepareSnapshotPreBarrier(checkpointId);
		// go forward through the operator chain and tell each operator
		// to prepare the checkpoint
		for (TableOperatorWrapper<?> wrapper : topologicalOrderingOperators) {
			if (!wrapper.isClosed()) {
				wrapper.getStreamOperator().prepareSnapshotPreBarrier(checkpointId);
			}
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		// go forward through the operator chain and tell each operator
		// to do snapshot
		for (TableOperatorWrapper<?> wrapper : topologicalOrderingOperators) {
			if (!wrapper.isClosed()) {
				StreamOperator<?> operator = wrapper.getStreamOperator();
				if (operator instanceof AbstractStreamOperator) {
					((AbstractStreamOperator<?>) operator).snapshotState(context);
				} else if (operator instanceof AbstractStreamOperatorV2) {
					((AbstractStreamOperatorV2<?>) operator).snapshotState(context);
				} else {
					throw new RuntimeException("Unsupported StreamOperator: " + operator);
				}
			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		// go forward through the operator chain and tell each operator
		// to notify checkpoint complete
		for (TableOperatorWrapper<?> wrapper : topologicalOrderingOperators) {
			if (!wrapper.isClosed()) {
				wrapper.getStreamOperator().notifyCheckpointComplete(checkpointId);
			}
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		super.notifyCheckpointAborted(checkpointId);
		// go back through the operator chain and tell each operator
		// to notify checkpoint aborted
		Iterator<TableOperatorWrapper<?>> it = topologicalOrderingOperators.descendingIterator();
		while (it.hasNext()) {
			TableOperatorWrapper<?> wrapper = it.next();
			if (!wrapper.isClosed()) {
				wrapper.getStreamOperator().notifyCheckpointAborted(checkpointId);
			}
		}
	}
}
