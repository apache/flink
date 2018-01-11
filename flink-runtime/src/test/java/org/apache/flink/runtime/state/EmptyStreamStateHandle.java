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

import java.io.IOException;

/**
 * A simple dummy implementation of a stream state handle that can be passed in tests.
 * The handle cannot open an input stream.
 */
public class EmptyStreamStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = 0L;

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void discardState() {}

	@Override
	public long getStateSize() {
		return 0;
	}
}
