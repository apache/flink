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

package org.apache.flink.runtime.io.network.partition.consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A combination of data, input and a flag indicating availability of further data.
 *
 * @param <INPUT> indicates a single channel or single gate.
 * @param <DATA> indicates a buffer or event.
 */
public class InputWithData<INPUT, DATA> {

	private final INPUT input;
	private final DATA data;
	private final boolean moreAvailable;

	InputWithData(INPUT input, DATA data, boolean moreAvailable) {
		this.input = checkNotNull(input);
		this.data = checkNotNull(data);
		this.moreAvailable = moreAvailable;
	}

	public INPUT input() {
		return input;
	}

	public DATA data() {
		return data;
	}

	public boolean moreAvailable() {
		return moreAvailable;
	}
}
