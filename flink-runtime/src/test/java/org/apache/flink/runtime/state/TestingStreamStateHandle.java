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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nullable;

/**
 * A simple test mock for a {@link StreamStateHandle}.
 */
public class TestingStreamStateHandle implements StreamStateHandle {
	private static final long serialVersionUID = 1L;

	@Nullable
	private final FSDataInputStream inputStream;

	private final long size;

	private boolean disposed;

	public TestingStreamStateHandle() {
		this(null, 0L);
	}

	public TestingStreamStateHandle(@Nullable FSDataInputStream inputStream, long size) {
		this.inputStream = inputStream;
		this.size = size;
	}

	// ------------------------------------------------------------------------

	@Override
	public FSDataInputStream openInputStream() {
		if (inputStream == null) {
			throw new UnsupportedOperationException("no input stream provided");
		}
		return inputStream;
	}

	@Override
	public void discardState() {
		disposed = true;
	}

	@Override
	public long getStateSize() {
		return size;
	}

	// ------------------------------------------------------------------------

	public boolean isDisposed() {
		return disposed;
	}
}
