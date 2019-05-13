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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that draws its data from a {@link DataInputView}.
 */
@Internal
public class DataInputViewStream extends InputStream {
	
	protected DataInputView inputView;

	public DataInputViewStream(DataInputView inputView) {
		this.inputView = inputView;
	}

	public DataInputView getInputView(){
		return inputView;
	}

	@Override
	public int read() throws IOException {
		try {
			return inputView.readUnsignedByte();
		} catch(EOFException ex) {
			return -1;
		}
	}

	@Override
	public long skip(long n) throws IOException {
		long toSkipRemaining = n;
		while(toSkipRemaining > Integer.MAX_VALUE) {
			int skippedBytes = inputView.skipBytes(Integer.MAX_VALUE);

			if (skippedBytes == 0) {
				return n - toSkipRemaining;
			}

			toSkipRemaining -= skippedBytes;
		}
		return n - (toSkipRemaining - inputView.skipBytes((int) toSkipRemaining));
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return inputView.read(b, off, len);
	}
}

