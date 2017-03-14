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

package org.apache.flink.streaming.util.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that contains helper methods to work with Flink Streaming
 * {@link Function Functions}. This is similar to
 * {@link org.apache.flink.api.common.functions.util.FunctionUtils} but has additional methods
 * for invoking interfaces that only exist in the streaming API.
 */
@Internal
public final class StreamingFunctionUtils {

	@SuppressWarnings("unchecked")
	public static <T> void setOutputType(
			Function userFunction,
			TypeInformation<T> outTypeInfo,
			ExecutionConfig executionConfig) {

		Preconditions.checkNotNull(outTypeInfo);
		Preconditions.checkNotNull(executionConfig);

		while (true) {
			if (trySetOutputType(userFunction, outTypeInfo, executionConfig)) {
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

	@SuppressWarnings("unchecked")
	private static  <T> boolean trySetOutputType(
			Function userFunction,
			TypeInformation<T> outTypeInfo,
			ExecutionConfig executionConfig) {

		Preconditions.checkNotNull(outTypeInfo);
		Preconditions.checkNotNull(executionConfig);

		if (OutputTypeConfigurable.class.isAssignableFrom(userFunction.getClass())) {
			((OutputTypeConfigurable<T>) userFunction).setOutputType(outTypeInfo, executionConfig);
			return true;
		}
		return false;
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
				try {
					for (Serializable statePartition : partitionableState) {
						listState.add(statePartition);
					}
				} catch (Exception e) {
					listState.clear();

					throw new Exception("Could not write partitionable state to operator " +
						"state backend.", e);
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

	/**
	 * Private constructor to prevent instantiation.
	 */
	private StreamingFunctionUtils() {
		throw new RuntimeException();
	}
}
