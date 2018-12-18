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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.PublicEvolving;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Utility class that turns an {@link OutputStream} into a {@link DataOutputView}.
 */
@PublicEvolving
public class DataOutputViewStreamWrapper extends DataOutputStream implements DataOutputView {

	private byte[] tempBuffer;

	public DataOutputViewStreamWrapper(OutputStream out) {
		super(out);
	}

	@Override
	public void skipBytesToWrite(int numBytes) throws IOException {
		if (tempBuffer == null) {
			tempBuffer = new byte[4096];
		}

		while (numBytes > 0) {
			int toWrite = Math.min(numBytes, tempBuffer.length);
			write(tempBuffer, 0, toWrite);
			numBytes -= toWrite;
		}
	}

	@Override
	public void write(DataInputView source, int numBytes) throws IOException {
		if (tempBuffer == null) {
			tempBuffer = new byte[4096];
		}

		while (numBytes > 0) {
			int toCopy = Math.min(numBytes, tempBuffer.length);
			source.readFully(tempBuffer, 0, toCopy);
			write(tempBuffer, 0, toCopy);
			numBytes -= toCopy;
		}
	}
}
