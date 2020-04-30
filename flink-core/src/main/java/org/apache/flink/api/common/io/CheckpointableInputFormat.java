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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.InputSplit;

import java.io.IOException;
import java.io.Serializable;

/**
 * An interface that describes {@link InputFormat}s that allow checkpointing/restoring their state.
 *
 * @param <S> The type of input split.
 * @param <T> The type of the channel state to be checkpointed / included in the snapshot.
 */
@PublicEvolving
public interface CheckpointableInputFormat<S extends InputSplit, T extends Serializable> {

	/**
	 * Returns the split currently being read, along with its current state.
	 * This will be used to restore the state of the reading channel when recovering from a task failure.
	 * In the case of a simple text file, the state can correspond to the last read offset in the split.
	 *
	 * @return The state of the channel.
	 *
	 * @throws IOException Thrown if the creation of the state object failed.
	 */
	T getCurrentState() throws IOException;

	/**
	 * Restores the state of a parallel instance reading from an {@link InputFormat}.
	 * This is necessary when recovering from a task failure. When this method is called,
	 * the input format it guaranteed to be configured.
	 *
	 * <p/>
	 * <b>NOTE: </b> The caller has to make sure that the provided split is the one to whom
	 * the state belongs.
	 *
	 * @param split The split to be opened.
	 * @param state The state from which to start from. This can contain the offset,
	 *                 but also other data, depending on the input format.
	 */
	void reopen(S split, T state) throws IOException;
}
