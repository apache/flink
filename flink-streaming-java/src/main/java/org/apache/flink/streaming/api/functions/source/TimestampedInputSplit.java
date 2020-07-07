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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.core.io.InputSplit;

import java.io.Serializable;

/**
 * An extended {@link InputSplit} that also includes information about:
 * <ul>
 *     <li>The modification time of the file this split belongs to.</li>
 *     <li>When checkpointing, the state of the split at the moment of the checkpoint.</li>
 * </ul>
 * This class is used by the {@link ContinuousFileMonitoringFunction} and the
 * {@link ContinuousFileReaderOperator} to perform continuous file processing.
 * */
public interface TimestampedInputSplit extends InputSplit, Comparable<TimestampedInputSplit> {

	/**
	 * Sets the state of the split. This information is used when restoring from a checkpoint and
	 * allows to resume reading the underlying file from the point we left off.
	 *
	 * <p>* This is applicable to
	 * {@link org.apache.flink.api.common.io.FileInputFormat FileInputFormats} that implement the
	 * {@link org.apache.flink.api.common.io.CheckpointableInputFormat} interface.
	 */
	void setSplitState(Serializable state);

	/**
	 * @return the state of the split.
	 */
	Serializable getSplitState();

	/**
	 * Sets the state of the split to {@code null}.
	 */
	default void resetSplitState() {
		this.setSplitState(null);
	}

	/**
	 * @return The modification time of the file this split belongs to.
	 */
	long getModificationTime();
}
