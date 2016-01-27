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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * InputSplitSources create {@link InputSplit}s that define portions of data to be produced
 * by {@link org.apache.flink.api.common.io.InputFormat}s.
 *
 * @param <T> The type of the input splits created by the source.
 */
@Public
public interface InputSplitSource<T extends InputSplit> extends Serializable {

	/**
	 * Computes the input splits. The given minimum number of splits is a hint as to how
	 * many splits are desired.
	 *
	 * @param minNumSplits Number of minimal input splits, as a hint.
	 * @return An array of input splits.
	 * 
	 * @throws Exception Exceptions when creating the input splits may be forwarded and will cause the
	 *                   execution to permanently fail.
	 */
	T[] createInputSplits(int minNumSplits) throws Exception;
	
	/**
	 * Returns the assigner for the input splits. Assigner determines which parallel instance of the
	 * input format gets which input split.
	 *
	 * @return The input split assigner.
	 */
	InputSplitAssigner getInputSplitAssigner(T[] inputSplits);
}
