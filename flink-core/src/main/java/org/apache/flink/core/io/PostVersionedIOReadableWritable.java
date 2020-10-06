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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;

/**
 * A {@link VersionedIOReadableWritable} which allows to differentiate whether the previous
 * data was versioned with a {@link VersionedIOReadableWritable}. This can be used if previously
 * written data was not versioned, and is to be migrated to a versioned format.
 */
@Internal
public abstract class PostVersionedIOReadableWritable extends VersionedIOReadableWritable {

	/** NOTE: CANNOT CHANGE! */
	private static final byte[] VERSIONED_IDENTIFIER = new byte[] {-15, -51, -123, -97};

	/**
	 * Read from the provided {@link DataInputView in}. A flag {@code wasVersioned} can be
	 * used to determine whether or not the data to read was previously written
	 * by a {@link VersionedIOReadableWritable}.
	 */
	protected abstract void read(DataInputView in, boolean wasVersioned) throws IOException;

	@Override
	public void write(DataOutputView out) throws IOException {
		out.write(VERSIONED_IDENTIFIER);
		super.write(out);
	}

	/**
	 * This read attempts to first identify if the input view contains the special
	 * {@link #VERSIONED_IDENTIFIER} by reading and buffering the first few bytes.
	 * If identified to be versioned, the usual version resolution read path
	 * in {@link VersionedIOReadableWritable#read(DataInputView)} is invoked.
	 * Otherwise, we "reset" the input stream by pushing back the read buffered bytes
	 * into the stream.
	 */
	public final void read(InputStream inputStream) throws IOException {
		byte[] tmp = new byte[VERSIONED_IDENTIFIER.length];
		int totalRead = IOUtils.tryReadFully(inputStream, tmp);

		if (Arrays.equals(tmp, VERSIONED_IDENTIFIER)) {
			DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

			super.read(inputView);
			read(inputView, true);
		} else {
			InputStream streamToRead = inputStream;
			if (totalRead > 0) {
				PushbackInputStream resetStream = new PushbackInputStream(inputStream, totalRead);
				resetStream.unread(tmp, 0, totalRead);
				streamToRead = resetStream;
			}

			read(new DataInputViewStreamWrapper(streamToRead), false);
		}
	}

	/**
	 * We do not support reading from a {@link DataInputView}, because it does not
	 * support pushing back already read bytes.
	 */
	@Override
	public final void read(DataInputView in) throws IOException {
		throw new UnsupportedOperationException("PostVersionedIOReadableWritable cannot read from a DataInputView.");
	}
}
