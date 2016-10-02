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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class TestByteStreamStateHandleDeepCompare extends ByteStreamStateHandle {

	private static final long serialVersionUID = -4946526195523509L;

	public TestByteStreamStateHandleDeepCompare(String handleName, byte[] data) {
		super(handleName, data);
	}

	@Override
	public boolean equals(Object o) {
		if (!super.equals(o)) {
			return false;
		}
		ByteStreamStateHandle other = (ByteStreamStateHandle) o;
		return Arrays.equals(getData(), other.getData());
	}

	@Override
	public int hashCode() {
		return 31 * super.hashCode() + Arrays.hashCode(getData());
	}

	public static StreamStateHandle fromSerializable(String handleName, Serializable value) throws IOException {
		return new TestByteStreamStateHandleDeepCompare(handleName, InstantiationUtil.serializeObject(value));
	}
}