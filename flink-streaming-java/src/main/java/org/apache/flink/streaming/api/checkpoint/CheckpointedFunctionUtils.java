/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for snapshot/restore in function checkpointing.
 */
public final class CheckpointedFunctionUtils {

	private CheckpointedFunctionUtils() {
		throw new AssertionError();
	}

	public static void snapshotFunctionState(
			StateSnapshotContext context,
			OperatorStateBackend backend,
			Function userFunction) throws Exception {

		Preconditions.checkNotNull(context);
		Preconditions.checkNotNull(backend);

		while (true) {

			if (trySnapshotFunctionState(context, backend, userFunction)) {
				break;
			}

			// inspect if the user function is wrapped, then unwrap and try again if we can snapshot the inner function
			if (userFunction instanceof WrappingFunction) {
				userFunction = ((WrappingFunction<?>) userFunction).getWrappedFunction();
			} else {
				break;
			}
		}
	}

	private static boolean trySnapshotFunctionState(
			StateSnapshotContext context,
			OperatorStateBackend backend,
			Function userFunction) throws Exception {

		if (userFunction instanceof CheckpointedFunction) {
			((CheckpointedFunction) userFunction).snapshotState(context);

			return true;
		}

		if (userFunction instanceof ListCheckpointed) {
			@SuppressWarnings("unchecked")
			List<Serializable> partitionableState = ((ListCheckpointed<Serializable>) userFunction).
					snapshotState(context.getCheckpointId(), context.getCheckpointTimestamp());

			ListState<Serializable> listState = backend.
					getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);

			listState.clear();

			if (null != partitionableState) {
				for (Serializable statePartition : partitionableState) {
					listState.add(statePartition);
				}
			}

			return true;
		}

		return false;
	}

	public static void restoreFunctionState(
			StateInitializationContext context,
			Function userFunction) throws Exception {

		Preconditions.checkNotNull(context);

		while (true) {

			if (tryRestoreFunction(context, userFunction)) {
				break;
			}

			// inspect if the user function is wrapped, then unwrap and try again if we can restore the inner function
			if (userFunction instanceof WrappingFunction) {
				userFunction = ((WrappingFunction<?>) userFunction).getWrappedFunction();
			} else {
				break;
			}
		}
	}

	private static boolean tryRestoreFunction(
			StateInitializationContext context,
			Function userFunction) throws Exception {

		if (userFunction instanceof CheckpointedFunction) {
			((CheckpointedFunction) userFunction).initializeState(context);

			return true;
		}

		if (context.isRestored() && userFunction instanceof ListCheckpointed) {
			@SuppressWarnings("unchecked")
			ListCheckpointed<Serializable> listCheckpointedFun = (ListCheckpointed<Serializable>) userFunction;

			ListState<Serializable> listState = context.getOperatorStateStore().
					getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);

			List<Serializable> list = new ArrayList<>();

			for (Serializable serializable : listState.get()) {
				list.add(serializable);
			}

			try {
				listCheckpointedFun.restoreState(list);
			} catch (Exception e) {

				throw new Exception("Failed to restore state to function: " + e.getMessage(), e);
			}

			return true;
		}

		return false;
	}
}