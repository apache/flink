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

package org.apache.flink.migration.v0.runtime.memory;

import org.apache.flink.migration.v0.runtime.StreamStateHandleV0;
import org.apache.flink.migration.v0.runtime.AbstractCloseableHandleV0;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * A handle to the states produced by {@code MemoryStateBackend.MemoryCheckpointStateOutputStream}
 * in SavepointV0.
 */
@Deprecated
@SuppressWarnings("deprecation")
public final class ByteStreamStateHandleV0 extends AbstractCloseableHandleV0 implements StreamStateHandleV0 {

	private static final long serialVersionUID = -5280226231200217594L;
	
	/** the state data */
	private final byte[] data;

	/**
	 * Creates a new ByteStreamStateHandle containing the given data.
	 * 
	 * @param data The state data.
	 */
	public ByteStreamStateHandleV0(byte[] data) {
		this.data = data;
	}

	public byte[] getData() {
		return data;
	}

	@Override
	public InputStream getState(ClassLoader userCodeClassLoader) throws Exception {
		return new ByteArrayInputStream(data);
	}
}
