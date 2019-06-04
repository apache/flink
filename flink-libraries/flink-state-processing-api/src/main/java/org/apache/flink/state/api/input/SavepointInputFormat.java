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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.SavepointLoader;

import java.io.IOException;

/**
 * Base input format for reading state form {@link Savepoint}'s.
 *
 * @param <OT> The type of the produced records.
 * @param <T> The type of input split.
 */
abstract class SavepointInputFormat<OT, T extends InputSplit> extends RichInputFormat<OT, T> {
	private final String savepointPath;

	protected final String uid;

	@SuppressWarnings("WeakerAccess")
	protected final OperatorID operatorID;

	SavepointInputFormat(String savepointPath, String uid) {
		this.savepointPath = savepointPath;
		this.uid = uid;
		this.operatorID = OperatorIDGenerator.fromUid(uid);
	}

	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return cachedStatistics;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	/**
	 * Finds the {@link OperatorState} for a uid within a savepoint.
	 *
	 * @return A handle to the operator state in the savepoint with the provided uid.
	 * @throws IOException If the savepoint path is invalid or the uid does not exist
	 */
	OperatorState getOperatorState() throws IOException {
		final Savepoint savepoint = SavepointLoader.loadSavepoint(savepointPath);

		for (final OperatorState state : savepoint.getOperatorStates()) {
			if (state.getOperatorID().equals(operatorID)) {
				return state;
			}
		}

		throw new IOException("Savepoint does not contain state with operator uid " + uid);
	}
}
