/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class FileRecord implements Record {

	private String fileName;

	private static final byte[] EMPTY_BYTES = new byte[0];

	private byte[] bytes;

	public FileRecord() {
		this.bytes = EMPTY_BYTES;
		fileName = "empty";
	}

	public FileRecord(final String fileName) {
		this.bytes = EMPTY_BYTES;
		this.fileName = fileName;
	}

	public void setFileName(final String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return this.fileName;
	}

	public byte[] getDataBuffer() {
		return this.bytes;
	}

	/**
	 * Append a range of bytes to the end of the given data.
	 * 
	 * @param data
	 *        the data to copy from
	 * @param start
	 *        the first position to append from data
	 * @param len
	 *        the number of bytes to append
	 */
	public void append(final byte[] data, final int start, final int len) {
		final int oldLength = this.bytes.length;
		setCapacity(this.bytes.length + len, true);
		System.arraycopy(data, start, this.bytes, oldLength, len);
	}

	private void setCapacity(final int len, final boolean keepData) {

		if (this.bytes == null || this.bytes.length < len) {
			final byte[] newBytes = new byte[len];
			if (this.bytes != null && keepData) {
				System.arraycopy(this.bytes, 0, newBytes, 0, this.bytes.length);
			}
			this.bytes = newBytes;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.fileName = StringRecord.readString(in);

		final int newLength = in.readInt();
		this.bytes = new byte[newLength];
		in.readFully(this.bytes, 0, newLength);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		StringRecord.writeString(out, fileName);
		out.writeInt(this.bytes.length);
		out.write(this.bytes, 0, this.bytes.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof FileRecord)) {
			return false;
		}

		final FileRecord fr = (FileRecord) obj;

		if (this.bytes.length != fr.bytes.length) {
			return false;
		}

		return Arrays.equals(this.bytes, fr.bytes);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return (int) ((11L * this.bytes.length) % Integer.MAX_VALUE);
	}
}
