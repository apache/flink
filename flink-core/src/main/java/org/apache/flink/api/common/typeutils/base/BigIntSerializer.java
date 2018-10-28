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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Serializer for serializing/deserializing BigInteger values including null values.
 */
@Internal
public final class BigIntSerializer extends TypeSerializerSingleton<BigInteger> {

	private static final long serialVersionUID = 1L;

	public static final BigIntSerializer INSTANCE = new BigIntSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public BigInteger createInstance() {
		return BigInteger.ZERO;
	}

	@Override
	public BigInteger copy(BigInteger from) {
		return from;
	}
	
	@Override
	public BigInteger copy(BigInteger from, BigInteger reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BigInteger record, DataOutputView target) throws IOException {
		writeBigInteger(record, target);
	}

	@Override
	public BigInteger deserialize(DataInputView source) throws IOException {
		return readBigInteger(source);
	}
	
	@Override
	public BigInteger deserialize(BigInteger reuse, DataInputView source) throws IOException {
		return readBigInteger(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		copyBigInteger(source, target);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BigIntSerializer;
	}

	// --------------------------------------------------------------------------------------------
	//                           Static Helpers for BigInteger Serialization
	// --------------------------------------------------------------------------------------------

	public static void writeBigInteger(BigInteger record, DataOutputView target) throws IOException {
		// null value support
		if (record == null) {
			target.writeInt(0);
			return;
		}
		// fast paths for 0, 1, 10
		// only reference equality is checked because equals would be too expensive
		else if (record == BigInteger.ZERO) {
			target.writeInt(1);
			return;
		}
		else if (record == BigInteger.ONE) {
			target.writeInt(2);
			return;
		}
		else if (record == BigInteger.TEN) {
			target.writeInt(3);
			return;
		}
		// default
		final byte[] bytes = record.toByteArray();
		// the length we write is offset by four, because null and short-paths for ZERO, ONE, and TEN
		target.writeInt(bytes.length + 4);
		target.write(bytes);
	}

	public static BigInteger readBigInteger(DataInputView source) throws IOException {
		final int len = source.readInt();
		if (len < 4) {
			switch (len) {
				case 0:
					return null;
				case 1:
					return BigInteger.ZERO;
				case 2:
					return BigInteger.ONE;
				case 3:
					return BigInteger.TEN;
			}
		}
		final byte[] bytes = new byte[len - 4];
		source.readFully(bytes);
		return new BigInteger(bytes);
	}

	public static boolean copyBigInteger(DataInputView source, DataOutputView target) throws IOException {
		final int len = source.readInt();
		target.writeInt(len);
		if (len > 4) {
			target.write(source, len - 4);
		}
		return len == 0; // returns true if the copied record was null
	}

	@Override
	public TypeSerializerSnapshot<BigInteger> snapshotConfiguration() {
		return new BigIntSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	public static final class BigIntSerializerSnapshot extends SimpleTypeSerializerSnapshot<BigInteger> {

		public BigIntSerializerSnapshot() {
			super(BigIntSerializer.class);
		}
	}
}
