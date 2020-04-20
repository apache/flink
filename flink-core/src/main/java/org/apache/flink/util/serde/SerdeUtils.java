/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.util.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * A util class helping the serialization of states.
 */
public class SerdeUtils {

	/** Private constructor for utility class. */
	private SerdeUtils() {}

	public static void writeInt(int value, ByteArrayOutputStream out) throws IOException {
		out.write(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	public static void writeLong(long value, ByteArrayOutputStream out) throws IOException {
		out.write(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}

	public static void writeString(String value, ByteArrayOutputStream out) throws IOException {
		byte[] bytes = value.getBytes(Charset.forName("UTF-8"));
		writeBytes(bytes, out);
	}

	public static void writeBytes(byte[] bytes, ByteArrayOutputStream out) throws IOException {
		writeInt(bytes.length, out);
		out.write(bytes);
	}

	public static int readInt(ByteArrayInputStream in) {
		return ByteBuffer.wrap(readBytes(in, Integer.BYTES)).getInt();
	}

	public static long readLong(ByteArrayInputStream in) {
		return ByteBuffer.wrap(readBytes(in, Long.BYTES)).getLong();
	}

	public static String readString(ByteArrayInputStream in) {
		int len = readInt(in);
		return new String(readBytes(in, len), Charset.forName("UTF-8"));
	}

	public static byte[] readBytes(ByteArrayInputStream in) {
		int len = readInt(in);
		return readBytes(in, len);
	}

	// --------- helper methods ------------------

	private static byte[] readBytes(ByteArrayInputStream in, int size) {
		byte[] bytes = new byte[size];
		int off = 0;
		// For ByteArrayInputStream the read should succeed with one shot.
		while (off < size) {
			int read = in.read(bytes, off, size - off);
			if (read < 0) {
				throw new BufferUnderflowException();
			}
			off += read;
		}
		return bytes;
	}
}
